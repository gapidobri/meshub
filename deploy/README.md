# Deploying meshub

This directory runs meshub in production with Docker Compose: a Mosquitto
broker that requires every client to log in and limits it to its own
topics, and the backend.

```
gateways ──1883──▶ mosquitto ◀── meshub backend ──8080──▶ web UI, HTTP API
                       ▲
applications ──────────┘ (MQTT, only their program's topics)
```

## 1. Configure

```sh
cd deploy
cp .env.example .env
```

Fill in `.env`:

* `MESHUB_API_TOKENS`: one token per application, comma separated, made
  with `openssl rand -hex 32`. Applications send one as
  `Authorization: Bearer <token>`.
* `MESHUB_UI_PASSWORD`: the web UI's password, for the user `admin`.
* `MESHUB_MQTT_PASSWORD`: the backend's broker password, which you set in
  the next step.
* `MESHUB_VERSION`: the image tag to run. Pin a version, such as `0.1.0`,
  rather than `latest`, so that upgrades happen when you choose. To run
  your own build instead, build it from the repository root with
  `docker build -t ghcr.io/gapidobri/meshub:dev .`, and set
  `MESHUB_VERSION=dev`.

Any other setting can be added to `.env` as a `MESHUB_*` variable. See
[`server/meshub.example.yaml`](../server/meshub.example.yaml) for the full
list.

## 2. Create broker users

Every client of the broker has a user in `mosquitto/passwd`. What each user
may do is set in [`mosquitto/acl`](mosquitto/acl).

```sh
# mosquitto_passwd from the broker image; ./mosquitto is /m inside it
mqpasswd() { docker run --rm -it -v "$PWD/mosquitto:/m" eclipse-mosquitto:2.0.22 mosquitto_passwd "$@"; }

mqpasswd -c /m/passwd meshub          # the backend: the password in .env
mqpasswd /m/passwd weather            # an application, named after its program
```

On Linux, let the broker's user read the file, and nobody else:

```sh
sudo chown 1883:1883 mosquitto/passwd && sudo chmod 0600 mosquitto/passwd
```

For each application, add a block to `mosquitto/acl` that gives its user
read access to its program's topics. The `weather` block there is an
example.

## 3. Start

```sh
docker compose up -d
docker compose logs -f meshub    # "mqtt connected", then "serving"
docker compose exec meshub /meshub identity
```

The last command prints the hub's public key. Devices are built with it.

The web UI and API are on port 8080. If they are reached from beyond the
host, put a TLS reverse proxy such as Caddy or nginx in front of them.
Without one, the tokens and passwords travel in clear text.

## 4. Add gateways

A gateway logs in to the broker with its public key as its user name, and
the access rules limit it to that gateway's topics. To add one:

1. Flash it, with the broker's LAN address in its config.
2. Read its public key from the serial console. It prints
   `gateway <PUBLIC KEY>` at boot.
3. Add the user, and reload the broker:

   ```sh
   mqpasswd /m/passwd <PUBLIC KEY>
   docker compose kill -s HUP mosquitto
   ```

4. Set the same key and password as `MQTTUser` and `MQTTPassword` in the
   gateway's config, and flash it again.

The gateway shows as online in the web UI once it connects.

The gateway firmware does not support TLS, so port 1883 must stay on the
network the gateways share with the broker, such as a LAN or a VPN. It must
not be exposed to the internet. Applications that connect from elsewhere
use a TLS listener instead: uncomment the 8883 listener in
`mosquitto/mosquitto.conf` and the matching lines in `compose.yaml`, and
put the certificate in `./certs`.

## Back up the database

The database holds the hub's private key. Every device is built with the
matching public key, so **if the database is lost, every device has to be
reflashed**. Take backups, and keep them somewhere other than the host.

A backup can be taken while the backend runs. It is written to
`./backups` on the host, which must be writable by the backend's user:

```sh
mkdir -p backups && sudo chown 65532:65532 backups   # once
docker compose exec meshub /meshub backup /backups/meshub-$(date +%F).db
# /backups/meshub-2026-10-28.db
# hub 39ADDC...
```

The command prints the hub key the copy holds. Check that it matches the
key your devices were built with. It never overwrites a file.

For daily backups, run the command from cron. Then copy `./backups` off
the host and delete old files, for example with `restic`, `rclone` or
`rsync`.

To restore a backup:

```sh
docker compose stop meshub
docker compose cp ./backups/meshub-2026-10-28.db meshub:/data/meshub.db
docker compose start meshub
```

## Upgrade

1. Read the [changelog](../CHANGELOG.md) for the new version.
2. Back up the database.
3. Set `MESHUB_VERSION` in `.env` to the new version.
4. Run `docker compose up -d`.

Database migrations run on start. On shutdown the backend answers the
devices it has already heard before it exits, which is why compose gives it
15 seconds.

## Health

* `/healthz` returns 200 while the process runs. The image's health check
  uses it.
* `/readyz` returns 200 when the database and the broker connection are
  both up, and 503 otherwise. Use it for load balancers and alerting.

The logs are JSON, one object per line.

## Limits

* **One backend per hub identity.** The backend keeps its state in SQLite,
  and the hub identity belongs to a single process. Run one instance, and
  rely on Docker's restart policy rather than replicas.
* **Shared instances are not isolated.** Every API token can read and
  change every device. The broker isolates applications' MQTT access, but
  the API does not. If several projects share one instance, they have to
  trust each other.
