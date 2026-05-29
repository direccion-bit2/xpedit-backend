"""
Tests for the POD signed-URL endpoint (GET /pod/{proof_id}/signed-urls).

This endpoint exists because the `proof-of-delivery` bucket is being made PRIVATE
(it was public-read = a live GDPR leak: anyone could list/download every delivery
photo, signature, name and GPS). POD media is now served only via short-lived
signed URLs minted server-side AFTER authorizing the caller against the proof's
route — the only design that also lets a same-company dispatcher view a driver's
POD when object paths are flat.
"""
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException

import main
from main import _pod_object_path, get_pod_signed_urls

# --- _pod_object_path: derive the storage object name from a stored url --------

def test_pod_object_path_from_public_url():
    url = "https://x.supabase.co/storage/v1/object/public/proof-of-delivery/proof_123_abc.jpg"
    assert _pod_object_path(url) == "proof_123_abc.jpg"


def test_pod_object_path_strips_signed_query():
    url = "https://x.supabase.co/storage/v1/object/sign/proof-of-delivery/sig_1.png?token=abc.def"
    assert _pod_object_path(url) == "sig_1.png"


def test_pod_object_path_bare_name():
    assert _pod_object_path("proof_9_z.jpg") == "proof_9_z.jpg"


def test_pod_object_path_none_and_foreign():
    assert _pod_object_path(None) is None
    assert _pod_object_path("") is None
    # A url to a different bucket/host with no marker → not extractable.
    assert _pod_object_path("https://evil.com/whatever.jpg") is None


# --- _pod_signed_url: mint a signed url via storage ----------------------------

def test_pod_signed_url_prefixes_relative_path():
    with patch.object(main, "supabase") as sb, \
         patch.object(main, "SUPABASE_URL", "https://x.supabase.co"):
        sb.storage.from_.return_value.create_signed_url.return_value = {
            "signedURL": "/storage/v1/object/sign/proof-of-delivery/proof_1.jpg?token=t"
        }
        out = main._pod_signed_url(
            "https://x.supabase.co/storage/v1/object/public/proof-of-delivery/proof_1.jpg"
        )
    assert out == "https://x.supabase.co/storage/v1/object/sign/proof-of-delivery/proof_1.jpg?token=t"


def test_pod_signed_url_absolute_passthrough():
    with patch.object(main, "supabase") as sb:
        sb.storage.from_.return_value.create_signed_url.return_value = {
            "signedUrl": "https://x.supabase.co/storage/v1/object/sign/proof-of-delivery/p.jpg?token=t"
        }
        out = main._pod_signed_url("proof-of-delivery/p.jpg")
    assert out.startswith("https://x.supabase.co/storage/v1/object/sign/")


def test_pod_signed_url_none_for_empty():
    assert main._pod_signed_url(None) is None


def test_pod_signed_url_swallows_storage_error():
    with patch.object(main, "supabase") as sb:
        sb.storage.from_.return_value.create_signed_url.side_effect = RuntimeError("boom")
        assert main._pod_signed_url("proof_x.jpg") is None


# --- endpoint: authorization + payload -----------------------------------------

def _proofs_table(proof_row):
    def dispatch(name):
        chain = MagicMock()
        if name == "delivery_proofs":
            res = MagicMock()
            res.data = [proof_row] if proof_row else []
            chain.select.return_value.eq.return_value.limit.return_value.execute.return_value = res
        return chain
    return dispatch


USER = {"id": "u1", "email": "d@x.com", "role": "driver", "company_id": None}


@pytest.mark.asyncio
async def test_pod_endpoint_404_when_missing():
    with patch.object(main, "supabase") as sb:
        sb.table.side_effect = _proofs_table(None)
        with pytest.raises(HTTPException) as exc:
            await get_pod_signed_urls("missing", USER)
    assert exc.value.status_code == 404


@pytest.mark.asyncio
async def test_pod_endpoint_authorizes_via_route_and_signs():
    proof = {
        "id": "p1", "route_id": "r1", "driver_id": "drv1",
        "photo_url": "https://x/object/public/proof-of-delivery/photo.jpg",
        "signature_url": None,
    }
    with patch.object(main, "supabase") as sb, \
         patch.object(main, "verify_route_access", new=AsyncMock(return_value={"id": "r1"})) as vra, \
         patch.object(main, "_pod_signed_url", side_effect=lambda u: f"signed::{u}" if u else None):
        sb.table.side_effect = _proofs_table(proof)
        out = await get_pod_signed_urls("p1", USER)
    vra.assert_awaited_once_with("r1", USER)
    assert out["photo_url"] == "signed::https://x/object/public/proof-of-delivery/photo.jpg"
    assert out["signature_url"] is None


@pytest.mark.asyncio
async def test_pod_endpoint_denied_propagates_403():
    proof = {"id": "p2", "route_id": "r2", "driver_id": "drv2", "photo_url": "x", "signature_url": None}
    with patch.object(main, "supabase") as sb, \
         patch.object(main, "verify_route_access",
                      new=AsyncMock(side_effect=HTTPException(status_code=403, detail="no"))):
        sb.table.side_effect = _proofs_table(proof)
        with pytest.raises(HTTPException) as exc:
            await get_pod_signed_urls("p2", USER)
    assert exc.value.status_code == 403


@pytest.mark.asyncio
async def test_pod_endpoint_falls_back_to_driver_when_no_route():
    proof = {"id": "p3", "route_id": None, "driver_id": "drv3", "photo_url": "x", "signature_url": "y"}
    with patch.object(main, "supabase") as sb, \
         patch.object(main, "verify_driver_access", new=AsyncMock(return_value=True)) as vda, \
         patch.object(main, "_pod_signed_url", side_effect=lambda u: f"s::{u}" if u else None):
        sb.table.side_effect = _proofs_table(proof)
        out = await get_pod_signed_urls("p3", USER)
    vda.assert_awaited_once_with("drv3", USER)
    assert out["photo_url"] == "s::x"
    assert out["signature_url"] == "s::y"


@pytest.mark.asyncio
async def test_pod_endpoint_403_when_no_route_and_no_driver():
    proof = {"id": "p4", "route_id": None, "driver_id": None, "photo_url": "x", "signature_url": None}
    with patch.object(main, "supabase") as sb:
        sb.table.side_effect = _proofs_table(proof)
        with pytest.raises(HTTPException) as exc:
            await get_pod_signed_urls("p4", USER)
    assert exc.value.status_code == 403
