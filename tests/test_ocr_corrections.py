"""Tests for the OCR learning loop (Day 2).

Coverage:
- /ocr/label and /ocr/screenshots-batch capture flow with/without consent
- PATCH /ocr/corrections/{id} ownership, consent gate, was_corrected logic
"""

from unittest.mock import MagicMock, patch

import pytest

# ============================================================================
# Helpers
# ============================================================================


def _gemini_label_resp(payload: dict) -> MagicMock:
    import json
    resp = MagicMock()
    resp.text = json.dumps(payload, ensure_ascii=False)
    return resp


def _patched_gemini_client(generate_return):
    client = MagicMock()
    client.models.generate_content.return_value = generate_return
    return client


# ============================================================================
# /ocr/label — consent capture
# ============================================================================


class TestOCRLabelConsentCapture:
    @pytest.mark.asyncio
    async def test_no_consent_skips_capture(self, client):
        """Default request (no consent flag): no driver lookup, no upload,
        no row creation. correction_id is null in the response."""
        payload = {
            "name": "Test", "street": "Calle 1", "city": "Madrid",
            "postalCode": "28001", "province": "Madrid",
        }
        gemini = _patched_gemini_client(_gemini_label_resp(payload))

        with patch("main.get_gemini_vertex_client", return_value=gemini), \
             patch("main._resolve_driver_id_from_user") as mock_resolve, \
             patch("main._upload_ocr_image_sync") as mock_upload, \
             patch("main._create_ocr_correction_row") as mock_create:
            resp = await client.post("/ocr/label", json={
                "image_base64": "dGVzdA==",
                "media_type": "image/jpeg",
            })

        assert resp.status_code == 200
        body = resp.json()
        assert body["correction_id"] is None
        mock_resolve.assert_not_called()
        mock_upload.assert_not_called()
        mock_create.assert_not_called()

    @pytest.mark.asyncio
    async def test_with_consent_uploads_and_creates_row(self, client):
        """With consent_to_training=true: driver lookup runs, image uploaded,
        ocr_corrections row created. correction_id returned to the app."""
        payload = {
            "name": "Maite", "street": "Calle 2", "city": "Bilbao",
            "postalCode": "48001", "province": "Bizkaia",
        }
        gemini = _patched_gemini_client(_gemini_label_resp(payload))

        with patch("main.get_gemini_vertex_client", return_value=gemini), \
             patch("main._resolve_driver_id_from_user", return_value="driver-uuid-1"), \
             patch("main._upload_ocr_image_sync", return_value="driver-uuid-1/label_scan/abc.jpg") as mock_upload, \
             patch("main._create_ocr_correction_row", return_value="correction-uuid-1") as mock_create:
            resp = await client.post("/ocr/label", json={
                "image_base64": "dGVzdA==",
                "media_type": "image/jpeg",
                "consent_to_training": True,
            })

        assert resp.status_code == 200
        body = resp.json()
        assert body["correction_id"] == "correction-uuid-1"
        mock_upload.assert_called_once()
        # The helper got driver_id + bytes + media_type + "label_scan" tag.
        assert mock_upload.call_args[0][0] == "driver-uuid-1"
        assert mock_upload.call_args[0][2] == "image/jpeg"
        assert mock_upload.call_args[0][3] == "label_scan"
        mock_create.assert_called_once()
        kwargs = mock_create.call_args.kwargs
        assert kwargs["consent"] is True
        assert kwargs["source"] == "label_scan"
        assert kwargs["model_extracted_parts"] == payload

    @pytest.mark.asyncio
    async def test_consent_but_no_driver_returns_response_without_id(self, client):
        """If the auth user has no driver row, we still return OCR data —
        we just can't attach it to anyone, so correction_id stays null."""
        payload = {"name": "X", "street": "Y", "city": "Z", "postalCode": "08001", "province": "Barcelona"}
        gemini = _patched_gemini_client(_gemini_label_resp(payload))

        with patch("main.get_gemini_vertex_client", return_value=gemini), \
             patch("main._resolve_driver_id_from_user", return_value=None), \
             patch("main._upload_ocr_image_sync") as mock_upload, \
             patch("main._create_ocr_correction_row") as mock_create:
            resp = await client.post("/ocr/label", json={
                "image_base64": "dGVzdA==",
                "media_type": "image/jpeg",
                "consent_to_training": True,
            })

        assert resp.status_code == 200
        assert resp.json()["correction_id"] is None
        mock_upload.assert_not_called()
        mock_create.assert_not_called()


# ============================================================================
# PATCH /ocr/corrections/{id}
# ============================================================================


class TestPatchOCRCorrectionAuth:
    @pytest.mark.asyncio
    async def test_patch_requires_auth(self, unauth_client):
        resp = await unauth_client.patch(
            "/ocr/corrections/some-id",
            json={"user_final_address": "X", "user_action": "accepted"},
        )
        assert resp.status_code in (401, 403)


class TestPatchOCRCorrectionOwnership:
    @pytest.mark.asyncio
    async def test_404_when_correction_not_found(self, client):
        """Missing correction id returns 404 (never reveals other drivers'
        ids)."""
        with patch("main._resolve_driver_id_from_user", return_value="driver-1"), \
             patch("main.supabase") as mock_sb:
            chain = MagicMock()
            chain.select.return_value = chain
            chain.eq.return_value = chain
            chain.limit.return_value = chain
            exec_result = MagicMock()
            exec_result.data = []
            chain.execute.return_value = exec_result
            mock_sb.table.return_value = chain

            resp = await client.patch(
                "/ocr/corrections/00000000-0000-0000-0000-000000000000",
                json={"user_final_address": "X", "user_action": "accepted"},
            )
        assert resp.status_code == 404

    @pytest.mark.asyncio
    async def test_404_when_correction_belongs_to_other_driver(self, client):
        """Foreign id with valid uuid is indistinguishable from 'not found'."""
        with patch("main._resolve_driver_id_from_user", return_value="driver-1"), \
             patch("main.supabase") as mock_sb:
            chain = MagicMock()
            chain.select.return_value = chain
            chain.eq.return_value = chain
            chain.limit.return_value = chain
            exec_result = MagicMock()
            exec_result.data = [{
                "id": "x",
                "driver_id": "OTHER-DRIVER",
                "model_extracted_address": "Y",
                "user_consented_training": True,
            }]
            chain.execute.return_value = exec_result
            mock_sb.table.return_value = chain

            resp = await client.patch(
                "/ocr/corrections/x",
                json={"user_final_address": "X", "user_action": "accepted"},
            )
        assert resp.status_code == 404


class TestPatchOCRCorrectionConsent:
    @pytest.mark.asyncio
    async def test_403_when_consent_not_recorded(self, client):
        """A correction row without recorded consent (e.g. legacy data) is
        not writable — refusing the update keeps every retained pair in
        the table backed by an explicit consent log entry."""
        with patch("main._resolve_driver_id_from_user", return_value="d1"), \
             patch("main.supabase") as mock_sb:
            chain = MagicMock()
            chain.select.return_value = chain
            chain.eq.return_value = chain
            chain.limit.return_value = chain
            exec_result = MagicMock()
            exec_result.data = [{
                "id": "x", "driver_id": "d1",
                "model_extracted_address": "Y",
                "user_consented_training": False,
            }]
            chain.execute.return_value = exec_result
            mock_sb.table.return_value = chain

            resp = await client.patch(
                "/ocr/corrections/x",
                json={"user_final_address": "X", "user_action": "accepted"},
            )
        assert resp.status_code == 403


class TestPatchOCRCorrectionWasCorrected:
    """The `was_corrected` flag drives downstream training: if False, the
    pair is a 'gold' confirmation (model was already right). The endpoint
    computes it server-side from a case-insensitive comparison so the
    client cannot lie."""

    def _setup_select_and_update(self, mock_sb, *, model_addr, consent=True):
        """Wire mock_sb.table() to return distinct chains for select vs update."""
        select_chain = MagicMock()
        select_chain.select.return_value = select_chain
        select_chain.eq.return_value = select_chain
        select_chain.limit.return_value = select_chain
        select_exec = MagicMock()
        select_exec.data = [{
            "id": "x", "driver_id": "d1",
            "model_extracted_address": model_addr,
            "user_consented_training": consent,
        }]
        select_chain.execute.return_value = select_exec

        update_chain = MagicMock()
        update_chain.update.return_value = update_chain
        update_chain.eq.return_value = update_chain
        update_chain.execute.return_value = MagicMock()

        # Same `.table()` call returns different chains on consecutive calls
        # (select first, update second).
        mock_sb.table.side_effect = [select_chain, update_chain]
        return select_chain, update_chain

    @pytest.mark.asyncio
    async def test_accepted_with_identical_address_is_not_corrected(self, client):
        with patch("main._resolve_driver_id_from_user", return_value="d1"), \
             patch("main.supabase") as mock_sb:
            _, update_chain = self._setup_select_and_update(
                mock_sb, model_addr="Calle Mayor 5, Madrid 28001"
            )
            resp = await client.patch(
                "/ocr/corrections/x",
                json={
                    "user_final_address": "Calle Mayor 5, Madrid 28001",
                    "user_action": "accepted",
                },
            )
        assert resp.status_code == 200
        body = resp.json()
        assert body["was_corrected"] is False
        # The update payload reflects the same flag.
        update_payload = update_chain.update.call_args[0][0]
        assert update_payload["was_corrected"] is False
        assert update_payload["user_action"] == "accepted"

    @pytest.mark.asyncio
    async def test_accepted_with_case_difference_is_not_corrected(self, client):
        """Case-only changes (Madrid vs MADRID) don't count as corrections."""
        with patch("main._resolve_driver_id_from_user", return_value="d1"), \
             patch("main.supabase") as mock_sb:
            self._setup_select_and_update(mock_sb, model_addr="Calle Mayor 5, Madrid")
            resp = await client.patch(
                "/ocr/corrections/x",
                json={
                    "user_final_address": "CALLE MAYOR 5, madrid",
                    "user_action": "accepted",
                },
            )
        assert resp.json()["was_corrected"] is False

    @pytest.mark.asyncio
    async def test_accepted_with_different_address_is_corrected(self, client):
        """Driver tapped 'accept' but the final string differs from the
        model — count it as a correction. Catches the UX path where the
        driver edits inline and then taps the same button as confirm."""
        with patch("main._resolve_driver_id_from_user", return_value="d1"), \
             patch("main.supabase") as mock_sb:
            self._setup_select_and_update(mock_sb, model_addr="Calle Mayor 5, Madrid")
            resp = await client.patch(
                "/ocr/corrections/x",
                json={
                    "user_final_address": "Calle Mayor 7, Madrid 28001",
                    "user_action": "accepted",
                },
            )
        assert resp.json()["was_corrected"] is True

    @pytest.mark.asyncio
    async def test_edited_is_always_corrected(self, client):
        """user_action=edited always sets was_corrected=true regardless of
        the strings (driver could have edited and then reverted)."""
        with patch("main._resolve_driver_id_from_user", return_value="d1"), \
             patch("main.supabase") as mock_sb:
            self._setup_select_and_update(mock_sb, model_addr="Same address")
            resp = await client.patch(
                "/ocr/corrections/x",
                json={
                    "user_final_address": "Same address",
                    "user_action": "edited",
                },
            )
        assert resp.json()["was_corrected"] is True
