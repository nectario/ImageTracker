# ADR-002: Deep Face Recognition Provider

Status: Accepted for future People implementation; no face processing is
enabled in Phase 1.

## Decision

Use Amazon Rekognition face collections and user vectors as the first
production `FaceRecognitionProvider`. GPT vision is not the identity-matching
engine.

- Maintain one Rekognition collection per ImageTracker user.
- Index multiple diverse face observations into a Rekognition user vector.
- Store ImageTracker asset/observation/person mappings and user corrections in
  MySQL.
- Never infer a person's name from the internet. A person receives a name only
  from the owning user.
- Remote media is processed from its private S3 original.
- Local media uses temporary private staging and deletes the image after
  provider processing; the provider vector and MySQL evidence may remain.
- Put the provider behind an interface so a commercially licensed, self-hosted
  detector/embedding stack can replace it later.

## Quality bar

The People feature is not complete with face detection alone. Release requires
deep detection, robust matching across pose/lighting/age, multiple exemplars,
quality thresholds, uncertain-match handling, and excellent merge/split/name
correction UX.

## Explicit exclusions

- Do not use GPT output as biometric identity evidence.
- Do not ship InsightFace-provided pretrained weights without a commercial
  license; the upstream project limits those weights to non-commercial research.
- Do not deploy an always-on SageMaker face endpoint for the first version.
