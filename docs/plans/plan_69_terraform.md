# Plan 69: Terraform — Infrastructure as Code

**Status:** Not started. **Repositioned 2026-08-20 from backlog to a stated
prerequisite of [Plan 121](plan_121_staging_environment.md).**

**Priority:** 66 (medium). **Effort:** M.

**Depends on:** nothing outstanding. The original "depends on Plan 68 (cloud
deployment)" is stale — the cloud deployment happened, the site is live, and the
VM survived a full shape migration in [Plan 105](plan_105_vm_migration.md).

## Why this moved out of the backlog

The backlog trigger was *"manual provisioning stops being stable **or a second
environment is approved**."* Plan 121 stands up `dev.cartracker.info`, which is
that second environment — so the trigger fires the moment Plan 121 starts.

Leaving Terraform behind Plan 121 gets the order exactly backwards: you
hand-build a second box, and then own two snowflakes instead of one. Doing it
first means staging and production are provisioned from the same modules, which
is both the correct engineering order and the only version of this work with a
demonstrable result.

There is a second, explicitly acknowledged reason. This is a portfolio project —
see `docs/reference/LINKEDIN_CASE_STUDY.md` and [Plan 138](plan_138_public_surface_refresh.md)
— and Terraform is high-demand, low-cost, and genuinely additive here. Unlike a
single-node Kubernetes migration (see [Plan 88's](../PLANS.md) backlog row), it
solves a real problem at the same time as it builds a marketable skill, so the
two motivations do not conflict. Recording that openly is better than
discovering it later in a commit message.

Currently cloud infrastructure is provisioned by clicking through the Oracle Cloud console. Terraform describes that same infrastructure in version-controlled `.tf` files — someone cloning the repo runs `terraform apply` and gets the identical environment.

## What gets described in Terraform
- Oracle Cloud VM (shape, size, OS image)
- Virtual network and subnet
- Firewall/security group rules (which ports are open)
- SSH key attachment
- DNS record (if using a custom domain)

## What Terraform does NOT manage
- Docker containers (that's Docker Compose's job)
- Application config (that's `.env`)
- Database schema (that's Plan 63)

The boundary is: Terraform provisions the machine, Docker Compose runs the software on it.

## Notes
- Terraform is free to use; Oracle Cloud provider is well-supported
- State file needs to be stored somewhere (Terraform Cloud free tier, or Oracle Object Storage)
- Can be developed and tested locally against Oracle Cloud without any cost beyond the free tier VM

## Importing the existing host, not recreating it

The production VM already exists, carries live data on `/mnt/data`, and must not
be destroyed to satisfy a plan file. The first slice is `terraform import` of the
current instance, network, and firewall rules until `terraform plan` reports **no
changes** against production.

That "no diff against the running system" state is the real gate. Only after it
holds should Plan 121's staging environment be created from the same modules —
at which point standing up staging is a parameter change rather than a second
manual build.

Note that the prod VM is ARM64 (OCI A1.Flex, see Plan 105); shape must be a
module input rather than a hardcoded value if staging is ever sized differently.
