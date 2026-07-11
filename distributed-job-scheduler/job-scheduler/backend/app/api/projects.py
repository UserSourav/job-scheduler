import secrets
import uuid

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.core.deps import get_current_user
from app.models.project import Project
from app.models.user import Organization, OrganizationMember, User
from app.models.enums import OrgRole
from app.schemas.project import (
    OrganizationCreate,
    OrganizationOut,
    ProjectCreate,
    ProjectOut,
)

router = APIRouter(prefix="/api/v1", tags=["projects"])


@router.post("/organizations", response_model=OrganizationOut, status_code=201)
async def create_organization(
    payload: OrganizationCreate,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    existing = (await db.execute(select(Organization).where(Organization.slug == payload.slug))).scalar_one_or_none()
    if existing:
        raise HTTPException(status_code=400, detail="Organization slug already exists")
    org = Organization(name=payload.name, slug=payload.slug)
    db.add(org)
    await db.flush()
    db.add(OrganizationMember(organization_id=org.id, user_id=current_user.id, role=OrgRole.OWNER))
    await db.commit()
    await db.refresh(org)
    return org


@router.get("/organizations", response_model=list[OrganizationOut])
async def list_organizations(
    db: AsyncSession = Depends(get_db), current_user: User = Depends(get_current_user)
):
    result = await db.execute(
        select(Organization)
        .join(OrganizationMember, OrganizationMember.organization_id == Organization.id)
        .where(OrganizationMember.user_id == current_user.id)
    )
    return result.scalars().all()


@router.post("/projects", response_model=ProjectOut, status_code=201)
async def create_project(
    payload: ProjectCreate,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    membership = (
        await db.execute(
            select(OrganizationMember).where(
                OrganizationMember.organization_id == payload.organization_id,
                OrganizationMember.user_id == current_user.id,
            )
        )
    ).scalar_one_or_none()
    if not membership:
        raise HTTPException(status_code=403, detail="Not a member of this organization")

    project = Project(
        organization_id=payload.organization_id,
        name=payload.name,
        slug=payload.slug,
        description=payload.description,
        api_key=secrets.token_hex(24),
    )
    db.add(project)
    await db.commit()
    await db.refresh(project)
    return project


@router.get("/projects", response_model=list[ProjectOut])
async def list_projects(
    organization_id: uuid.UUID | None = None,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    query = (
        select(Project)
        .join(OrganizationMember, OrganizationMember.organization_id == Project.organization_id)
        .where(OrganizationMember.user_id == current_user.id)
    )
    if organization_id:
        query = query.where(Project.organization_id == organization_id)
    result = await db.execute(query)
    return result.scalars().all()


@router.get("/projects/{project_id}", response_model=ProjectOut)
async def get_project(
    project_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    project = (await db.execute(select(Project).where(Project.id == project_id))).scalar_one_or_none()
    if not project:
        raise HTTPException(status_code=404, detail="Project not found")
    return project
