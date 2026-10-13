# Telegram Controller Bot (Python + MySQL + Render)

## Included
- Telegram polling worker suitable for Render Background Worker
- User registration in a dedicated `controller_users` table; best-effort compatible insert into legacy `users`
- Force-subscription gate (configure `FORCE_SUB_CHANNEL` and `FORCE_SUB_URL`)
- Add/manage channels using `/addchannel @channelusername`
- Text/photo/video post drafts, URL buttons in a two-column grid, immediate publishing
- Scheduled publishing stored in `scheduled_posts`
- Admin analytics, user/channel lists, ban/unban, support reply, broadcast to users/channels/both
- Additive `CREATE TABLE IF NOT EXISTS` migrations only; no DROP/TRUNCATE/ALTER statements

## Security first
The bot token previously shared in chat should be revoked in BotFather and replaced.
Never commit secrets to GitHub. Set them in Render Environment settings.
Use a private repository if desired, and restrict database access to Render's IP/network where possible.
Use TLS for MySQL if your provider requires it. Use a dedicated DB account with only required permissions.
Do not expose database credentials or bot tokens in logs/screenshots.

## Deploy on Render
1. Create a GitHub repository and upload `main.py`, `requirements.txt`, `render.yaml`, and this README.
2. In Render choose **New + → Blueprint** and select the repository, or create a **Background Worker**.
3. Add environment variables:
   - `BOT_TOKEN`: NEW token from BotFather
   - `ADMIN_ID`: `6913254882`
   - `MYSQL_HOST`, `MYSQL_PORT`, `MYSQL_USER`, `MYSQL_PASSWORD`, `MYSQL_DATABASE`
   - `FORCE_SUB_CHANNEL`: public `@channelusername` (optional)
   - `FORCE_SUB_URL`: `https://t.me/channelusername` (optional)
   - `TIMEZONE`: use `UTC` for simplest scheduling
4. Ensure the bot is admin in every managed channel with permission to post. It must be able to check membership in the force-sub channel.
5. Deploy and inspect Render logs.

## Commands
- `/start`
- `/addchannel @channelusername`
- `/addbutton Button_Name https://example.com` (repeat for each button; each line is laid out two buttons wide)
- `/publish`
- `/schedule YYYY-MM-DD HH:MM`

## Important existing database note
The exact schemas of the legacy `users`, `channels`, and `user_states` tables were not provided. This project never alters or drops them. It records data in separate `controller_users`, `controller_channels`, and `controller_user_states` tables. It also makes a best-effort insert into the legacy `users` table only when recognizable ID/name columns are found. Before using existing `channels` or `user_states`, adapt their integration after reviewing their actual schema. Back up the database before first deployment.

## Current limitations to review before production
- This is a usable starter implementation, not a security-audited enterprise release.
- Draft composition is in process memory and is lost if Render restarts before publishing/scheduling.
- URL shortening is not configured; add a trusted provider API key and endpoint before enabling automatic shortening.
- Broadcast to channels requires the bot to be admin with posting rights. Telegram privacy/blocking errors are counted as failures.
- For high-volume broadcasts, add a durable queue/worker and global Telegram rate limiting.
- Scheduling uses UTC timestamps. Set schedule times in UTC or implement an explicit local timezone conversion before production.
- MySQL user must have SELECT/INSERT/UPDATE/CREATE privileges for these additive tables.


## Render Web Service + `/health`

This version starts an HTTP server on `0.0.0.0:$PORT` and exposes `GET /health` (JSON, HTTP 200). `render.yaml` configures the service as a Web Service and sets `healthCheckPath: /health`. Set `BOT_TOKEN`, `MYSQL_HOST`, `MYSQL_PORT`, `MYSQL_USER`, `MYSQL_PASSWORD`, and `MYSQL_DATABASE` in Render Environment Variables. Never commit real secrets.

**Free-tier note:** a free web service may sleep when idle; a health endpoint or uptime monitor does not guarantee 24/7 operation. Use an always-on plan if continuous uptime is required.
