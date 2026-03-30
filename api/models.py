"""Pydantic models for API request/response."""

from typing import Optional
from pydantic import BaseModel


class PhaseStatus(BaseModel):
    phase: str
    status: str
    progress_current: int
    progress_total: int
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    error_message: Optional[str] = None


class ClusterLabel(BaseModel):
    person_label: str


class MergeRequest(BaseModel):
    source_cluster_id: int
    target_cluster_id: int


class FaceReassignRequest(BaseModel):
    source_cluster_id: int
    face_ids: list[int]
    target_cluster_id: Optional[int] = None
    target_person_label: Optional[str] = None


class FaceSelectionRequest(BaseModel):
    face_ids: list[int]


class AcceptSuggestionRequest(BaseModel):
    person_label: str
    source_cluster_id: Optional[int] = None


class DetectionAction(BaseModel):
    detection_id: int


class VocabEntry(BaseModel):
    tag_group: str
    tag_name: str
    prompts: list[str]
    enabled: bool = True


class SettingsUpdate(BaseModel):
    nas_source_dir: Optional[str] = None
    local_base: Optional[str] = None
    batch_manifest_path: Optional[str] = None
    yolo_conf_threshold: Optional[float] = None
    clip_tag_threshold: Optional[float] = None
    max_inference_dim: Optional[int] = None
    det_thresh: Optional[float] = None
    umap_n_neighbors: Optional[int] = None
    hdbscan_min_cluster_size: Optional[int] = None
    hdbscan_min_samples: Optional[int] = None


class SavedSearchCreate(BaseModel):
    name: str
    query: dict
