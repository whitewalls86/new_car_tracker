# Plan 68: Cloud Deployment — Oracle Free Tier

**Status:** Not started
**Priority:** Medium — prerequisite for Plans 65 (auth), 69 (Terraform), and public sharing

Move the project from a local home server to Oracle Cloud's Always Free tier. Oracle's free tier is the most generous available — 4 Ampere ARM cores, 24GB RAM, 200GB storage, no time limit. The full stack runs comfortably on it with no cost.

## Why Oracle Free Tier
- Genuinely free forever — not a trial
- Enough resources to run all 8 containers without compromise
- Real cloud infrastructure: VMs, networking, firewall rules, DNS
- Gives portfolio evidence of cloud deployment without a credit card

## Work involved
- Provision VM, networking, and firewall rules (manually first, then Terraform in Plan 69)
- Configure DNS if sharing publicly
- Update `docker-compose.yml` for ARM architecture if needed (most images have ARM builds)
- Move `.env` secrets to cloud VM securely
- Set up SSH access and deploy workflow (`git pull + docker compose build + up`)
- Coordinate with Plan 65 (auth) — don't expose publicly without authentication in place

## Notes
- Plans 65 (auth) and 66 (SQL injection audit) should be completed before exposing any port publicly
- Oracle ARM architecture is broadly compatible with standard Docker images; any exceptions need ARM-specific builds
