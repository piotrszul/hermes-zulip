# hermes-zulip

A [Zulip](https://zulip.com) gateway platform plugin for
[Hermes Agent](https://github.com/NousResearch/hermes-agent). It connects a
Zulip bot to Hermes so you can talk to your agent in Zulip streams, topics and
direct messages.

- Works with Zulip Cloud and self-hosted servers through the v1 REST API and
  the long-polling event queue. Its only dependency is `aiohttp`; no Zulip SDK
  is needed.
- Topics work like Slack threads: each topic and each direct-message partner
  gets its own Hermes session.
- Follows topics like Slack: once the bot has posted in a topic, it answers
  every message there. Elsewhere in a stream it needs an @-mention. Direct
  messages always reach the agent.
- Can be limited to a single stream.
- Downloads attachments sent to the bot and uploads files the agent sends.
- Splits replies longer than Zulip's 10,000-character limit.
- Delivers cron output to a stream, opening a new topic for each run, so you
  can reply to a scheduled brief in its own topic.
- Works under a multi-profile gateway (`gateway.multiplex_profiles`): each
  profile signs in as its own bot.

## Requirements

- Hermes Agent 0.21.6 or later
- A Zulip [bot account](https://zulip.com/help/add-a-bot-or-integration)
  (a "Generic bot" is enough), with its email and API key

## Installation

```sh
hermes plugins install johngrimes/hermes-zulip --enable
```

Hermes asks you to approve installing `aiohttp` (if it isn't already present)
and prompts for the required settings. To use the plugin in another profile,
install it there too (`hermes -p <profile> plugins install …`).

## Configuration

Set these in the profile's `.env` (or any secret source Hermes supports):

| Variable | Required | Description |
| --- | --- | --- |
| `ZULIP_SITE` | yes | Organisation URL, e.g. `https://example.zulipchat.com` |
| `ZULIP_BOT_EMAIL` | yes | Bot account email |
| `ZULIP_BOT_API_KEY` | yes | Bot API key |
| `ZULIP_STREAM` | no | Name of the only stream the bot responds in. Without it, the bot responds in every stream it is subscribed to. |
| `ZULIP_ALLOWED_USERS` | no | Comma-separated Zulip user IDs allowed to talk to the bot |
| `ZULIP_ALLOW_ALL_USERS` | no | `true` lets any user talk to the bot (development only) |
| `ZULIP_HOME_CHANNEL` | no | Target for Hermes status notices and for cron jobs that name no target (see below). Leave it unset to send no notices to Zulip (Hermes then suggests `/sethome` at the start of each new conversation). |
| `ZULIP_REQUIRE_MENTION` | no | Require an @-mention in topics the bot has not posted in (default `true`) |
| `ZULIP_FREE_RESPONSE_TOPICS` | no | Comma-separated topics where no @-mention is ever needed |

The last three non-secret settings can go in `config.yaml` instead; environment
variables take precedence:

```yaml
zulip:
  stream: hermes
  require_mention: true
  free_response_topics:
    - chat
```

The bot counts as mentioned when Zulip flags the message as mentioning it, or
when the text contains `@` followed by its username (the part of its email
before `@`) or full name.

### Topics and following

Hermes sees a stream as a chat and each topic as a thread in it. A message in a
topic where the bot has posted reaches Hermes without a mention, so you can
carry on a conversation the bot started, or one you started with a mention.
After a restart the bot asks Zulip whether it has posted in a topic, so nothing
extra is stored.

### Delivery targets

Use these targets for cron jobs (`deliver`), `send_message` and
`ZULIP_HOME_CHANNEL`. A stream can be given by ID or by name.

| Target | Delivers to |
| --- | --- |
| `zulip:<stream>` | A new topic for each cron run, named `<job name> · YYYY-MM-DD HH:MM` in the host's local time. Other messages go to the topic `Hermes`. |
| `zulip:<stream>:<topic>` | That topic, every time |
| `zulip:dm:<user_id>` | A direct message |

Zulip only creates a topic when something is posted to it, so a cron run that
stays silent leaves no empty topic. If a topic name is already taken, the bot
adds ` (2)`, ` (3)` and so on.

To reply to a cron brief in its topic with the brief in context, enable
`attach_to_session` on the job (or `cron.mirror_delivery` in `config.yaml`).

### Upgrading from earlier versions

Topics used to be separate chats (`zulip:<stream_id>:<topic>`); they are now
threads of the stream chat. Existing topic conversations start a fresh Hermes
session once after upgrading. Old-style targets keep working.

## Updating

```sh
hermes plugins update zulip-platform
```

## Development

The tests import Hermes's gateway modules, so run them inside a Hermes
checkout's environment ([uv](https://docs.astral.sh/uv/) and Python 3.14):

```sh
git clone https://github.com/NousResearch/hermes-agent.git ../hermes-agent
uv sync --project ../hermes-agent --frozen
uv run --project ../hermes-agent --frozen --with pytest pytest
```

CI runs the tests and `hermes plugins validate --install-deps` against the
oldest supported Hermes release (0.21.6) and Hermes `main`.

## License

[MIT](LICENSE)
