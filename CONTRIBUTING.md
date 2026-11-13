# Contributing to plugin-jmsq

Thanks for helping. This guide covers how to report problems, propose changes, and get a pull request merged.

By taking part you agree to follow the [Code of Conduct](CODE_OF_CONDUCT.md).

## Ways to contribute

- **Report a bug:** open an issue with the *Bug report* form. Include the broker and version, the connector, the settings you used (with passwords removed), and the log output.
- **Suggest a feature:** open an issue with the *Feature request* form before writing code, so we can agree on the approach.
- **Ask a question:** use [GitHub Discussions](https://github.com/OWNER/plugin-jmsq/discussions) rather than an issue.
- **Report a security problem:** do **not** open an issue. Follow [SECURITY.md](SECURITY.md).

## Development setup

You need:

- JDK 8, 11, 17 or 21. The code targets Java 8; CI builds on all four.
- Git. Maven isn't needed separately, because the wrapper (`./mvnw`) downloads it.
- Optional, for manual testing: Docker, to run brokers locally.

```sh
git clone https://github.com/OWNER/plugin-jmsq.git
cd plugin-jmsq
./mvnw verify          # compile, test, package (Windows: mvnw.cmd verify)
```

Start test brokers locally:

```sh
docker run -d --name activemq -p 61616:61616 -p 8161:8161 apache/activemq-classic:5.18.3
docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management
docker run -d --name kafka    -p 9092:9092 apache/kafka:3.7.0
```

Then run the smoke tests in `AMQJMSConnection.main` and `TestQueue.main` (see the README).

## Making a change

1. Fork the repository and create a branch from `main`, named for the change, for example `fix/rabbit-reconnect` or `feat/kafka-headers`.
2. Keep each pull request to one change. Refactoring and behaviour changes go in separate pull requests.
3. Follow the existing style:
   - Java 8 language level only (no `var`, no records).
   - Tabs for indentation in Java and XML; see `.editorconfig`.
   - Every new file starts with the Apache License header (copy it from an existing file).
   - Every public class and method gets a Javadoc comment that says what it does, its parameters, and what it returns on failure.
   - Log through Log4j 2 (`LogManager.getLogger`), not `System.out`.
   - Never commit credentials, hostnames or IP addresses. Use `examples/application.properties.example` for sample settings.
4. Add or update tests where you can. Code that needs a running broker may be tested manually; say how in the pull request.
5. Update `README.md` and `CHANGELOG.md` (under *Unreleased*) when behaviour or configuration changes.
6. Run `./mvnw verify` and make sure it passes.

### Commit messages

Use the [Conventional Commits](https://www.conventionalcommits.org/) style:

```
fix(rabbit): replace closed channels on next use

A channel closed by the broker stayed cached, so every later send to that
queue failed until restart.
```

Common types: `feat`, `fix`, `docs`, `refactor`, `test`, `build`, `ci`, `chore`.

### Sign-off (DCO)

Sign off every commit to certify that you wrote it or have the right to submit it under the project's license, as defined by the [Developer Certificate of Origin](https://developercertificate.org/):

```sh
git commit -s -m "fix(kafka): ..."
```

This adds a `Signed-off-by: Your Name <you@example.com>` line.

## Pull request review

- CI must pass on every JDK in the matrix.
- A maintainer reviews within about a week. Address comments with new commits; we squash on merge.
- Changes to public method signatures need a clear reason and a CHANGELOG entry under **Changed** or **Removed**.

## Releasing (maintainers)

Releases go to Maven Central through the [Central Portal](https://central.sonatype.com/).

One-time setup:

1. Verify a namespace on the Central Portal and change `groupId` in `pom.xml` to it. Central rejects `com.plugin`.
2. Create a GPG key and publish it to a key server (`gpg --keyserver keys.openpgp.org --send-keys <id>`).
3. Generate a Central Portal user token and add it to `~/.m2/settings.xml`:

   ```xml
   <servers>
     <server>
       <id>central</id>
       <username>TOKEN_USER</username>
       <password>TOKEN_PASSWORD</password>
     </server>
   </servers>
   ```

For each release:

1. Set the version: `./mvnw versions:set -DnewVersion=X.Y.Z -DgenerateBackupPoms=false`.
2. Move the *Unreleased* notes in `CHANGELOG.md` under a new `X.Y.Z` heading with today's date.
3. Commit, then tag: `git tag -s vX.Y.Z -m "plugin-jmsq X.Y.Z"`.
4. Publish: `./mvnw -Prelease clean deploy`. This builds sources and Javadoc jars, signs everything, and uploads; then confirm the deployment on the Central Portal.
5. Push the commit and tag, and create a GitHub release from the tag with the CHANGELOG notes. Attach `target/plugin-jmsq-X.Y.Z-shaded.jar` from a normal `./mvnw package` build.
6. Set the next development version, for example `X.Y.(Z+1)-SNAPSHOT`.

## Contact

Maintainer: SKACT Tech, [skact.tech@gmail.com](mailto:skact.tech@gmail.com).
