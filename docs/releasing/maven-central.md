# Maven Central release

[English](maven-central.md) | [한국어](maven-central.ko.md)

EventDock publishes all seven modules through the Central Publisher Portal. A version tag is immutable and starts the release workflow.

## Required repository secrets

Configure these GitHub Actions secrets:

- `MAVEN_CENTRAL_USERNAME`: Central Portal user-token username
- `MAVEN_CENTRAL_PASSWORD`: Central Portal user-token password
- `SIGNING_KEY`: complete ASCII-armored PGP private key
- `SIGNING_PASSWORD`: PGP private-key passphrase

The `io.github.oxxultus` namespace must be verified in the Central Portal. Publish the matching public PGP key to a keyserver supported by Maven Central. Never store credentials or private-key material in Git, workflow inputs, logs, or command-line arguments.

## Release procedure

1. Confirm `main` CI passes and the working tree is clean.
2. Update both changelogs by moving relevant entries from `Unreleased` to the release version and date.
3. Create an annotated semantic-version tag on the intended `main` commit.
4. Push the tag.
5. Verify the `Release to Maven Central` workflow and Central Portal deployment.
6. Confirm every artifact is available from Maven Central before updating installation examples.

```shell
git switch main
git pull --ff-only
git tag -a v0.1.0 -m "Release 0.1.0"
git push origin v0.1.0
```

The workflow derives `0.1.0` from `v0.1.0`, builds and tests that version, stages seven signed publications, validates the staged file count, and uploads the deployment through JReleaser. Tags not matching semantic version syntax fail before publication.

## Failed release

Do not reuse a version that Maven Central has published. Fix the problem and release the next version. If validation fails before publication, inspect the uploaded `jreleaser-<version>` diagnostic artifact and the Central Portal deployment. Delete an incorrect local or remote tag only when the version was not published.

Maven Central releases cannot be modified or removed after publication.
