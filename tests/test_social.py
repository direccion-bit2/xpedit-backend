"""
Tests for social media management endpoints:
  - GET /social/posts
  - POST /social/posts
  - PUT /social/posts/{post_id}
  - DELETE /social/posts/{post_id}
  - POST /social/posts/{post_id}/publish
  - GET /social/accounts
  - POST /social/generate-text
  - POST /social/generate-image
  - POST /social/generate-calendar
"""

import pytest
from unittest.mock import MagicMock, patch, AsyncMock


class TestSocialAccessControl:
    """Social endpoints require admin auth."""

    @pytest.mark.asyncio
    async def test_social_posts_requires_admin(self, client):
        """Regular driver should get 403 on social posts."""
        response = await client.get("/social/posts")
        assert response.status_code == 403

    @pytest.mark.asyncio
    async def test_create_social_post_requires_admin(self, client):
        """Regular driver should get 403 on create social post."""
        response = await client.post("/social/posts", json={
            "content": "Test post",
            "platforms": ["twitter"],
        })
        assert response.status_code == 403

    @pytest.mark.asyncio
    async def test_generate_text_requires_admin(self, client):
        """Regular driver should get 403 on generate text."""
        response = await client.post("/social/generate-text", json={
            "topic": "feature",
        })
        assert response.status_code == 403

    @pytest.mark.asyncio
    async def test_generate_image_requires_admin(self, client):
        """Regular driver should get 403 on generate image."""
        response = await client.post("/social/generate-image", json={
            "prompt": "A delivery truck",
        })
        assert response.status_code == 403

    @pytest.mark.asyncio
    async def test_social_accounts_requires_admin(self, client):
        """Regular driver should get 403 on social accounts."""
        response = await client.get("/social/accounts")
        assert response.status_code == 403


class TestSocialPostsCRUD:
    """Tests for social post CRUD operations."""

    @pytest.mark.asyncio
    async def test_list_social_posts(self, admin_client):
        """Admin should see all social posts."""
        with patch("main.supabase") as mock_sb:
            posts_result = MagicMock()
            posts_result.data = [
                {"id": "p1", "content": "Post 1", "status": "draft", "platforms": ["twitter"]},
                {"id": "p2", "content": "Post 2", "status": "published", "platforms": ["twitter", "linkedin"]},
            ]

            def table_dispatch(name):
                chain = MagicMock()
                if name == "social_posts":
                    chain.select.return_value.order.return_value.limit.return_value.execute.return_value = posts_result
                return chain

            mock_sb.table = MagicMock(side_effect=table_dispatch)

            response = await admin_client.get("/social/posts")

        assert response.status_code == 200
        data = response.json()
        assert len(data) == 2

    @pytest.mark.asyncio
    async def test_list_social_posts_filtered_by_status(self, admin_client):
        """Filter social posts by status."""
        with patch("main.supabase") as mock_sb:
            posts_result = MagicMock()
            posts_result.data = [
                {"id": "p1", "content": "Draft", "status": "draft"},
            ]

            def table_dispatch(name):
                chain = MagicMock()
                if name == "social_posts":
                    chain.select.return_value.order.return_value.eq.return_value.limit.return_value.execute.return_value = posts_result
                    # Also handle: select().order().limit() without eq()
                    chain.select.return_value.order.return_value.limit.return_value.execute.return_value = posts_result
                return chain

            mock_sb.table = MagicMock(side_effect=table_dispatch)

            response = await admin_client.get("/social/posts?status=draft")

        assert response.status_code == 200

    @pytest.mark.asyncio
    async def test_create_social_post_draft(self, admin_client):
        """Creating a post without scheduled_at should create a draft."""
        with patch("main.supabase") as mock_sb:
            insert_result = MagicMock()
            insert_result.data = [{
                "id": "new-post",
                "content": "Hello world!",
                "platforms": ["twitter"],
                "status": "draft",
            }]

            def table_dispatch(name):
                chain = MagicMock()
                if name == "social_posts":
                    chain.insert.return_value.execute.return_value = insert_result
                return chain

            mock_sb.table = MagicMock(side_effect=table_dispatch)

            response = await admin_client.post("/social/posts", json={
                "content": "Hello world!",
                "platforms": ["twitter"],
            })

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "draft"
        assert data["content"] == "Hello world!"

    @pytest.mark.asyncio
    async def test_create_social_post_scheduled(self, admin_client):
        """Creating a post with scheduled_at should create a scheduled post."""
        with patch("main.supabase") as mock_sb:
            insert_result = MagicMock()
            insert_result.data = [{
                "id": "new-post",
                "content": "Scheduled post",
                "platforms": ["twitter", "linkedin"],
                "status": "scheduled",
                "scheduled_at": "2026-02-20T10:00:00",
            }]

            def table_dispatch(name):
                chain = MagicMock()
                if name == "social_posts":
                    chain.insert.return_value.execute.return_value = insert_result
                return chain

            mock_sb.table = MagicMock(side_effect=table_dispatch)

            response = await admin_client.post("/social/posts", json={
                "content": "Scheduled post",
                "platforms": ["twitter", "linkedin"],
                "scheduled_at": "2026-02-20T10:00:00",
            })

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "scheduled"

    @pytest.mark.asyncio
    async def test_update_social_post(self, admin_client):
        """Updating a draft post should succeed."""
        with patch("main.supabase") as mock_sb:
            # Existing post check
            existing = MagicMock()
            existing.data = {"status": "draft"}

            # Update result
            update_result = MagicMock()
            update_result.data = [{
                "id": "p1",
                "content": "Updated content",
                "status": "draft",
            }]

            call_count = {"social_posts": 0}

            def table_dispatch(name):
                chain = MagicMock()
                if name == "social_posts":
                    call_count["social_posts"] += 1
                    if call_count["social_posts"] == 1:
                        chain.select.return_value.eq.return_value.single.return_value.execute.return_value = existing
                    else:
                        chain.update.return_value.eq.return_value.execute.return_value = update_result
                return chain

            mock_sb.table = MagicMock(side_effect=table_dispatch)

            response = await admin_client.put("/social/posts/p1", json={
                "content": "Updated content",
            })

        assert response.status_code == 200

    @pytest.mark.asyncio
    async def test_update_published_post_rejected(self, admin_client):
        """Cannot edit a published post."""
        with patch("main.supabase") as mock_sb:
            existing = MagicMock()
            existing.data = {"status": "published"}

            def table_dispatch(name):
                chain = MagicMock()
                if name == "social_posts":
                    chain.select.return_value.eq.return_value.single.return_value.execute.return_value = existing
                return chain

            mock_sb.table = MagicMock(side_effect=table_dispatch)

            response = await admin_client.put("/social/posts/p1", json={
                "content": "Try to update",
            })

        assert response.status_code == 400

    @pytest.mark.asyncio
    async def test_update_nonexistent_post(self, admin_client):
        """Updating a nonexistent post should return 404."""
        with patch("main.supabase") as mock_sb:
            existing = MagicMock()
            existing.data = None

            def table_dispatch(name):
                chain = MagicMock()
                if name == "social_posts":
                    chain.select.return_value.eq.return_value.single.return_value.execute.return_value = existing
                return chain

            mock_sb.table = MagicMock(side_effect=table_dispatch)

            response = await admin_client.put("/social/posts/nonexistent", json={
                "content": "Ghost post",
            })

        assert response.status_code == 404

    @pytest.mark.asyncio
    async def test_delete_draft_post(self, admin_client):
        """Deleting a draft post should succeed."""
        with patch("main.supabase") as mock_sb:
            existing = MagicMock()
            existing.data = {"status": "draft", "image_urls": []}

            def table_dispatch(name):
                chain = MagicMock()
                if name == "social_posts":
                    chain.select.return_value.eq.return_value.single.return_value.execute.return_value = existing
                    chain.delete.return_value.eq.return_value.execute.return_value = MagicMock()
                return chain

            mock_sb.table = MagicMock(side_effect=table_dispatch)

            response = await admin_client.delete("/social/posts/p1")

        assert response.status_code == 200
        assert response.json()["status"] == "deleted"

    @pytest.mark.asyncio
    async def test_delete_published_post_rejected(self, admin_client):
        """Cannot delete a published post."""
        with patch("main.supabase") as mock_sb:
            existing = MagicMock()
            existing.data = {"status": "published", "image_urls": []}

            def table_dispatch(name):
                chain = MagicMock()
                if name == "social_posts":
                    chain.select.return_value.eq.return_value.single.return_value.execute.return_value = existing
                return chain

            mock_sb.table = MagicMock(side_effect=table_dispatch)

            response = await admin_client.delete("/social/posts/p1")

        assert response.status_code == 400

    @pytest.mark.asyncio
    async def test_delete_nonexistent_post(self, admin_client):
        """Deleting a nonexistent post should return 404."""
        with patch("main.supabase") as mock_sb:
            existing = MagicMock()
            existing.data = None

            def table_dispatch(name):
                chain = MagicMock()
                if name == "social_posts":
                    chain.select.return_value.eq.return_value.single.return_value.execute.return_value = existing
                return chain

            mock_sb.table = MagicMock(side_effect=table_dispatch)

            response = await admin_client.delete("/social/posts/nonexistent")

        assert response.status_code == 404


class TestSocialAccounts:
    """Tests for GET /social/accounts"""

    @pytest.mark.asyncio
    async def test_list_social_accounts(self, admin_client):
        """Admin should see all connected social accounts."""
        with patch("main.supabase") as mock_sb:
            accounts_result = MagicMock()
            accounts_result.data = [
                {"id": "a1", "platform": "twitter", "username": "@Xpedit_es"},
            ]

            def table_dispatch(name):
                chain = MagicMock()
                if name == "social_accounts":
                    chain.select.return_value.execute.return_value = accounts_result
                return chain

            mock_sb.table = MagicMock(side_effect=table_dispatch)

            response = await admin_client.get("/social/accounts")

        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1
        assert data[0]["platform"] == "twitter"


class TestGenerateText:
    """Tests for POST /social/generate-text"""

    @pytest.mark.asyncio
    async def test_generate_text_success(self, admin_client):
        """Should return generated text when Gemini is configured."""
        mock_gemini = MagicMock()
        mock_response = MagicMock()
        mock_response.text = '{"twitter_text": "Test tweet #Xpedit", "linkedin_text": "Longer post for LinkedIn", "hashtags": ["Xpedit"], "image_prompt": "A delivery truck"}'

        mock_gemini.models.generate_content.return_value = mock_response

        with patch("main.get_gemini_client", return_value=mock_gemini):
            response = await admin_client.post("/social/generate-text", json={
                "topic": "feature",
                "platforms": ["twitter", "linkedin"],
                "tone": "profesional",
            })

        assert response.status_code == 200
        data = response.json()
        assert "twitter_text" in data
        assert "linkedin_text" in data
        assert "hashtags" in data
        assert "image_prompt" in data

    @pytest.mark.asyncio
    async def test_generate_text_custom_topic(self, admin_client):
        """Custom topic should be passed to the prompt."""
        mock_gemini = MagicMock()
        mock_response = MagicMock()
        mock_response.text = '{"twitter_text": "Custom", "linkedin_text": "Custom long", "hashtags": ["custom"], "image_prompt": "Custom image"}'

        mock_gemini.models.generate_content.return_value = mock_response

        with patch("main.get_gemini_client", return_value=mock_gemini):
            response = await admin_client.post("/social/generate-text", json={
                "topic": "custom",
                "custom_topic": "Announce new feature X",
                "platforms": ["twitter"],
            })

        assert response.status_code == 200

    @pytest.mark.asyncio
    async def test_generate_text_no_gemini(self, admin_client):
        """Should return 500 when Gemini is not configured."""
        with patch("main.get_gemini_client", return_value=None):
            response = await admin_client.post("/social/generate-text", json={
                "topic": "feature",
            })

        assert response.status_code == 500
        assert "Gemini" in response.json()["detail"]

    @pytest.mark.asyncio
    async def test_generate_text_gemini_error(self, admin_client):
        """Should return 500 when Gemini returns an error."""
        mock_gemini = MagicMock()
        mock_gemini.models.generate_content.side_effect = Exception("API Error")

        with patch("main.get_gemini_client", return_value=mock_gemini):
            response = await admin_client.post("/social/generate-text", json={
                "topic": "feature",
            })

        assert response.status_code == 500

    @pytest.mark.asyncio
    async def test_generate_text_invalid_json_response(self, admin_client):
        """Should return 500 when Gemini returns invalid JSON."""
        mock_gemini = MagicMock()
        mock_response = MagicMock()
        mock_response.text = "This is not JSON"

        mock_gemini.models.generate_content.return_value = mock_response

        with patch("main.get_gemini_client", return_value=mock_gemini):
            response = await admin_client.post("/social/generate-text", json={
                "topic": "feature",
            })

        assert response.status_code == 500


class TestGenerateImage:
    """Tests for POST /social/generate-image"""

    @pytest.mark.asyncio
    async def test_generate_image_success(self, admin_client):
        """Should return image URL when generation succeeds."""
        mock_gemini = MagicMock()
        mock_image = MagicMock()
        mock_image.image.image_bytes = b"\x89PNG\r\n\x1a\nfake_image_data"
        mock_response = MagicMock()
        mock_response.generated_images = [mock_image]

        mock_gemini.models.generate_images.return_value = mock_response

        with patch("main.get_gemini_client", return_value=mock_gemini):
            with patch("main.supabase") as mock_sb:
                bucket = MagicMock()
                bucket.upload.return_value = None
                mock_sb.storage.from_.return_value = bucket

                response = await admin_client.post("/social/generate-image", json={
                    "prompt": "A delivery truck in a Spanish city",
                    "aspect_ratio": "1:1",
                    "style": "flat",
                })

        assert response.status_code == 200
        data = response.json()
        assert "url" in data
        assert "prompt_used" in data
        assert "filename" in data

    @pytest.mark.asyncio
    async def test_generate_image_no_gemini(self, admin_client):
        """Should return 500 when Gemini is not configured."""
        with patch("main.get_gemini_client", return_value=None):
            response = await admin_client.post("/social/generate-image", json={
                "prompt": "A delivery truck",
            })

        assert response.status_code == 500

    @pytest.mark.asyncio
    async def test_generate_image_no_result(self, admin_client):
        """Should return 500 when no image is generated."""
        mock_gemini = MagicMock()
        mock_response = MagicMock()
        mock_response.generated_images = []

        mock_gemini.models.generate_images.return_value = mock_response

        with patch("main.get_gemini_client", return_value=mock_gemini):
            response = await admin_client.post("/social/generate-image", json={
                "prompt": "A delivery truck",
            })

        assert response.status_code == 500


class TestGenerateCalendar:
    """Tests for POST /social/generate-calendar"""

    @pytest.mark.asyncio
    async def test_generate_calendar_success(self, admin_client):
        """Should return a calendar of posts."""
        mock_gemini = MagicMock()
        calendar_json = '{"posts": [{"twitter_text": "Post 1", "linkedin_text": "Long 1", "suggested_date": "2026-02-18", "suggested_time": "10:00", "image_prompt": "img1", "theme": "feature", "hashtags": ["tag1"]}]}'
        mock_response = MagicMock()
        mock_response.text = calendar_json

        mock_gemini.models.generate_content.return_value = mock_response

        with patch("main.get_gemini_client", return_value=mock_gemini):
            response = await admin_client.post("/social/generate-calendar", json={
                "days": 3,
                "posts_per_day": 1,
                "platforms": ["twitter", "linkedin"],
                "themes": ["feature", "tip"],
            })

        assert response.status_code == 200
        data = response.json()
        assert "posts" in data
        assert len(data["posts"]) >= 1

    @pytest.mark.asyncio
    async def test_generate_calendar_no_gemini(self, admin_client):
        """Should return 500 when Gemini is not configured."""
        with patch("main.get_gemini_client", return_value=None):
            response = await admin_client.post("/social/generate-calendar", json={
                "days": 7,
            })

        assert response.status_code == 500
