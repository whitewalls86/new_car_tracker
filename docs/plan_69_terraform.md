# Plan 69: Terraform — Infrastructure as Code

**Status:** Not started
**Priority:** Medium — depends on Plan 68 (cloud deployment)

**Depends on:** Plan 68

Currently cloud infrastructure (if any) would be provisioned by clicking through the Oracle Cloud console. Terraform describes that same infrastructure in version-controlled `.tf` files — someone cloning the repo runs `terraform apply` and gets the identical environment.

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
