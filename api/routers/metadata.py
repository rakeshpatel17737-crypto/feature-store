from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from api.dependencies import get_db
from registry.registry_service import get_all_features, get_feature
from registry.schemas import FeatureDefinitionResponse

router = APIRouter(tags=["metadata"])


@router.get("/feature-metadata", response_model=list[FeatureDefinitionResponse])
async def list_feature_metadata(db: AsyncSession = Depends(get_db)):
    return await get_all_features(db)


@router.get("/feature-metadata/{feature_name}", response_model=FeatureDefinitionResponse)
async def get_feature_metadata(feature_name: str, db: AsyncSession = Depends(get_db)):
    feature = await get_feature(db, feature_name)
    if not feature:
        raise HTTPException(status_code=404, detail=f"Feature '{feature_name}' not found")
    return feature
