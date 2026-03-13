# Slack 봇 연동 가이드

## 1. Slack App 생성

1. [Slack API](https://api.slack.com/apps)에서 새 앱 생성
2. **OAuth & Permissions**에서 Bot Token Scopes 추가:
   - `app_mentions:read`
   - `chat:write`
   - `commands`

3. **Event Subscriptions** 활성화:
   - `app_mention` 이벤트 구독

4. **Slash Commands** 추가:
   - Command: `/sqe`
   - Description: 자연어로 데이터베이스 쿼리

## 2. 토큰 획득

| 환경 변수 | 위치 |
|----------|------|
| `SLACK_BOT_TOKEN` | OAuth & Permissions → Bot User OAuth Token (xoxb-...) |
| `SLACK_SIGNING_SECRET` | Basic Information → Signing Secret |
| `SLACK_APP_TOKEN` | Basic Information → App-Level Tokens (xapp-..., `connections:write` scope 필요) |

## 3. 환경 변수 설정

`.env` 파일에 추가:
```env
SLACK_BOT_TOKEN=xoxb-your-bot-token
SLACK_SIGNING_SECRET=your-signing-secret
SLACK_APP_TOKEN=xapp-your-app-token
```

## 4. 실행

```bash
# Slack 봇 실행
.venv\Scripts\activate
python -m semantic_engine.slack_bot
```

## 5. 사용법

Slack에서:
- `@봇이름 지난 달 매출 TOP 10` (멘션)
- `/sqe 이번 주 신규 가입자 수` (슬래시 커맨드)
