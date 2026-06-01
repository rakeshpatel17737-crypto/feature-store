from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from .database import get_async_session
from .registry_service import get_all_features, get_feature
from .schemas import FeatureDefinitionResponse

router = APIRouter(prefix="/registry", tags=["registry"])


@router.get("/features", response_model=list[FeatureDefinitionResponse])
async def list_features(db: AsyncSession = Depends(get_async_session)):
    return await get_all_features(db)


@router.get("/features/{feature_name}", response_model=FeatureDefinitionResponse)
async def get_feature_detail(feature_name: str, db: AsyncSession = Depends(get_async_session)):
    feature = await get_feature(db, feature_name)
    if not feature:
        raise HTTPException(status_code=404, detail=f"Feature '{feature_name}' not found")
    return feature
