import uuid

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.core.deps import get_current_user
from app.models.queue import RetryPolicy
from app.models.user import User
from app.schemas.queue import RetryPolicyCreate, RetryPolicyOut

router = APIRouter(prefix="/api/v1/retry-policies", tags=["retry-policies"])


@router.post("", response_model=RetryPolicyOut, status_code=201)
async def create_retry_policy(
    payload: RetryPolicyCreate, db: AsyncSession = Depends(get_db), current_user: User = Depends(get_current_user)
):
    policy = RetryPolicy(**payload.model_dump())
    db.add(policy)
    await db.commit()
    await db.refresh(policy)
    return policy


@router.get("", response_model=list[RetryPolicyOut])
async def list_retry_policies(
    db: AsyncSession = Depends(get_db), current_user: User = Depends(get_current_user)
):
    result = await db.execute(select(RetryPolicy))
    return result.scalars().all()


@router.get("/{policy_id}", response_model=RetryPolicyOut)
async def get_retry_policy(
    policy_id: uuid.UUID, db: AsyncSession = Depends(get_db), current_user: User = Depends(get_current_user)
):
    policy = (await db.execute(select(RetryPolicy).where(RetryPolicy.id == policy_id))).scalar_one_or_none()
    if not policy:
        raise HTTPException(status_code=404, detail="Retry policy not found")
    return policy
