from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime
from enum import Enum


# --- Categories ---
# Dynamically mapped from AI across 46 taxonomy classes


# --- Location ---

class Location(BaseModel):
    name: str
    address: Optional[str] = None
    neighborhood: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    country: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None


# --- Request Models ---

class ReelInput(BaseModel):
    url: str = Field(..., description="Instagram reel URL")
    user_id: str = Field(default="default-user", description="User identifier")


class SearchQuery(BaseModel):
    query: str = Field(..., description="Natural language search query")
    user_id: str = Field(default="default-user", description="Filter by user")
    category: Optional[str] = Field(None, description="Filter by category")
    subcategory: Optional[str] = Field(None, description="Filter by subcategory")
    limit: int = Field(default=5, ge=1, le=20, description="Max results")


class DevicePushTokenInput(BaseModel):
    user_id: str = Field(..., description="Authenticated user identifier")
    token: str = Field(..., description="Firebase Cloud Messaging token")
    platform: str = Field(..., description="ios, android, or web")


class ProactiveRecallPushRequest(BaseModel):
    user_id: str = Field(..., description="Target user identifier")
    title: str = Field(..., description="Notification title")
    body: str = Field(..., description="Notification body")
    data: dict[str, str] = Field(default_factory=dict, description="Optional string data payload")


# --- Extracted Data ---

class ExtractedData(BaseModel):
    title: str = ""
    summary: str = ""
    category: str = "Other"
    subcategory: str = "Other"
    secondary_categories: list[str] = Field(default_factory=list)
    key_facts: list[str] = Field(default_factory=list)
    locations: list[Location] = Field(default_factory=list)
    people_mentioned: list[str] = Field(default_factory=list)
    actionable_items: list[str] = Field(default_factory=list)


# --- Response Models ---

class ReelResponse(BaseModel):
    id: str
    user_id: str
    url: str
    title: str
    summary: str
    transcript: str
    category: str
    subcategory: str = "Other"
    secondary_categories: list[str] = Field(default_factory=list)
    key_facts: list[str] = Field(default_factory=list)
    locations: list[Location] = Field(default_factory=list)
    people_mentioned: list[str] = Field(default_factory=list)
    actionable_items: list[str] = Field(default_factory=list)
    created_at: Optional[str] = None


class SearchResult(BaseModel):
    reel: ReelResponse
    relevance_score: float


class SearchResponse(BaseModel):
    query: str
    results: list[SearchResult]
    total: int


class HealthResponse(BaseModel):
    status: str = "ok"
    version: str = "1.0.0"
    service: str = "ReelMind API"


class GenericSuccessResponse(BaseModel):
    success: bool = True
    message: str = "ok"
