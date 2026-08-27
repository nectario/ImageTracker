from __future__ import annotations

from datetime import datetime, timezone

import pytest

from infra.scripts.live_phase1_smoke import (
    AcceptanceError,
    DisposableCognitoUser,
    _bucket_snapshot,
    _create_confirmed_user,
    _safe_error,
    _stack_outputs,
)


class FakePaginator:
    def __init__(self, pages):
        self.pages = pages
        self.calls = []

    def paginate(self, **kwargs):
        self.calls.append(kwargs)
        return self.pages


class FakeS3:
    def __init__(self, pages):
        self.paginator = FakePaginator(pages)

    def get_paginator(self, operation):
        assert operation == "list_objects_v2"
        return self.paginator


class FakeCloudFormation:
    def describe_stacks(self, **kwargs):
        assert kwargs == {"StackName": "image-tracker-prod"}
        return {
            "Stacks": [
                {
                    "StackStatus": "UPDATE_COMPLETE",
                    "Outputs": [
                        {"OutputKey": key, "OutputValue": value}
                        for key, value in {
                            "ImageTrackerHttpApiUrl": "https://api.example",
                            "CognitoUserPoolId": "pool",
                            "CognitoUserPoolClientId": "client",
                            "MediaBucketName": "bucket",
                            "ConfigurationParameterPrefix": "/imagetracker/prod",
                        }.items()
                    ],
                }
            ]
        }


class FakeCognito:
    def __init__(self):
        self.create_request = None
        self.password_request = None

    def admin_create_user(self, **kwargs):
        self.create_request = kwargs
        return {
            "User": {
                "Username": "internal-user",
                "Attributes": [{"Name": "sub", "Value": "subject-123"}],
            }
        }

    def admin_set_user_password(self, **kwargs):
        self.password_request = kwargs

    def admin_get_user(self, **kwargs):
        assert kwargs == {"UserPoolId": "pool", "Username": "internal-user"}
        return {"UserStatus": "CONFIRMED", "UserAttributes": []}


def test_stack_discovery_requires_and_returns_phase1_outputs():
    status, outputs = _stack_outputs(
        FakeCloudFormation(), stack_name="image-tracker-prod"
    )

    assert status == "UPDATE_COMPLETE"
    assert outputs["MediaBucketName"] == "bucket"


def test_bucket_snapshot_is_content_sensitive_without_exposing_keys():
    pages = [
        {
            "Contents": [
                {
                    "Key": "private/original.jpg",
                    "ETag": '"abc"',
                    "Size": 12,
                    "LastModified": datetime(2026, 8, 27, tzinfo=timezone.utc),
                }
            ]
        }
    ]
    s3 = FakeS3(pages)

    snapshot = _bucket_snapshot(s3, bucket="bucket")

    assert snapshot.object_count == 1
    assert snapshot.total_bytes == 12
    assert len(snapshot.fingerprint_sha256) == 64
    assert "private" not in str(snapshot.as_json())
    assert s3.paginator.calls == [{"Bucket": "bucket"}]


def test_disposable_cognito_user_is_suppressed_and_confirmed():
    cognito = FakeCognito()
    state = DisposableCognitoUser(
        email="imagetracker-smoke@example.com", password="private-password"
    )

    _create_confirmed_user(cognito, user_pool_id="pool", user_state=state)

    assert state.username == "internal-user"
    assert state.subject == "subject-123"
    assert cognito.create_request["MessageAction"] == "SUPPRESS"
    assert cognito.password_request["Permanent"] is True


def test_failure_output_does_not_echo_unexpected_exception_text():
    rendered = _safe_error(RuntimeError("do-not-print-this-secret"))

    assert "do-not-print-this-secret" not in rendered
    assert rendered.startswith("RuntimeError:")


def test_stack_discovery_rejects_non_ready_stack():
    cloudformation = FakeCloudFormation()
    original = cloudformation.describe_stacks

    def describe_stacks(**kwargs):
        response = original(**kwargs)
        response["Stacks"][0]["StackStatus"] = "UPDATE_IN_PROGRESS"
        return response

    cloudformation.describe_stacks = describe_stacks

    with pytest.raises(AcceptanceError, match="not ready"):
        _stack_outputs(cloudformation, stack_name="image-tracker-prod")
