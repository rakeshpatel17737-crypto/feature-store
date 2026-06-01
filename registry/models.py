from __future__ import annotations

from datetime import datetime

from sqlalchemy import (
    Boolean, Column, DateTime, Float, ForeignKey,
    Integer, String, Text, func,
)
from sqlalchemy.orm import DeclarativeBase, relationship


class Base(DeclarativeBase):
    pass


class FeatureDefinition(Base):
    __tablename__ = "feature_definitions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    feature_name = Column(String(100), unique=True, nullable=False, index=True)
    display_name = Column(String(200))
    description = Column(Text)
    transform_logic = Column(Text)
    window_type = Column(String(20))
    freshness_sla_seconds = Column(Integer, default=300)
    owner = Column(String(100))
    version = Column(String(20), default="1.0")
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())

    lineage = relationship("FeatureLineage", back_populates="feature", cascade="all, delete-orphan")
    metrics = relationship("FeatureMetric", back_populates="feature", cascade="all, delete-orphan")


class FeatureLineage(Base):
    __tablename__ = "feature_lineage"

    id = Column(Integer, primary_key=True, autoincrement=True)
    feature_name = Column(String(100), ForeignKey("feature_definitions.feature_name"), nullable=False)
    upstream_source = Column(String(200))
    transform_step = Column(String(200))
    created_at = Column(DateTime, default=func.now())

    feature = relationship("FeatureDefinition", back_populates="lineage")


class FeatureMetric(Base):
    __tablename__ = "feature_metrics"

    id = Column(Integer, primary_key=True, autoincrement=True)
    feature_name = Column(String(100), ForeignKey("feature_definitions.feature_name"), nullable=False)
    metric_type = Column(String(50))  # freshness | drift_score | psi | null_rate | consistency_rate
    metric_value = Column(Float)
    recorded_at = Column(DateTime, default=func.now(), index=True)

    feature = relationship("FeatureDefinition", back_populates="metrics")


class RCAResult(Base):
    __tablename__ = "rca_results"

    id = Column(Integer, primary_key=True, autoincrement=True)
    feature_name = Column(String(100), nullable=False, index=True)
    probable_cause = Column(Text)
    cause_category = Column(String(50))
    confidence = Column(Float)
    urgency = Column(String(20))
    remediation_steps = Column(Text)  # JSON list stored as text
    estimated_impact = Column(Text)
    model_used = Column(String(100))
    tokens_used = Column(Integer)
    computed_at = Column(DateTime, default=func.now(), index=True)
