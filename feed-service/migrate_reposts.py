#!/usr/bin/env python3
"""
Миграция: создаёт записи в feed_posts (is_repost=True) для всех
существующих feed_reposts, у которых ещё нет записи в feed_posts.

Запуск: venv/bin/python migrate_reposts.py
"""
import os
import sys
import uuid
import json
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).parent))
load_dotenv()

from app.db.pool import ydb_pool

REPOSTS_TABLE = "feed_reposts"
POSTS_TABLE = "feed_posts"


def to_ts(dt: datetime) -> int:
    return int(dt.timestamp() * 1_000_000)


def get_all_reposts(session):
    query = f"""
    SELECT repost_id, original_post_id, user_id, comment, created_at
    FROM {REPOSTS_TABLE};
    """
    result = session.transaction().execute(query, commit_tx=True)
    return result[0].rows if result and result[0].rows else []


def get_original_post(session, post_id: str):
    query = f"""
    DECLARE $post_id AS Utf8;
    SELECT content, user_id, created_at FROM {POSTS_TABLE}
    WHERE post_id = $post_id AND is_deleted = false;
    """
    prepared = session.prepare(query)
    result = session.transaction().execute(prepared, {'$post_id': post_id}, commit_tx=True)
    rows = result[0].rows if result and result[0].rows else []
    return rows[0] if rows else None


def repost_feed_post_exists(session, user_id: str, original_post_id: str) -> bool:
    """Проверяем, есть ли уже feed_posts запись для этого репоста."""
    query = f"""
    DECLARE $user_id AS Utf8;
    DECLARE $original_post_id AS Utf8;
    SELECT post_id FROM {POSTS_TABLE}
    WHERE user_id = $user_id
      AND original_post_id = $original_post_id
      AND is_repost = true
      AND is_deleted = false;
    """
    prepared = session.prepare(query)
    result = session.transaction().execute(
        prepared,
        {'$user_id': user_id, '$original_post_id': original_post_id},
        commit_tx=True,
    )
    rows = result[0].rows if result and result[0].rows else []
    return len(rows) > 0


def create_feed_post_for_repost(session, user_id: str, original_post_id: str,
                                 comment: str, created_at_ts: int):
    post_id = str(uuid.uuid4())
    comment = comment or ''
    preview = comment[:200] + ('...' if len(comment) > 200 else '')

    query = f"""
    DECLARE $post_id AS Utf8;
    DECLARE $user_id AS Utf8;
    DECLARE $content AS Utf8;
    DECLARE $content_preview AS Utf8;
    DECLARE $media_urls AS Json;
    DECLARE $reactions AS Json;
    DECLARE $created_at AS Timestamp;
    DECLARE $updated_at AS Timestamp;
    DECLARE $comments_count AS Uint32;
    DECLARE $reposts_count AS Uint32;
    DECLARE $views_count AS Uint32;
    DECLARE $bookmarks_count AS Uint32;
    DECLARE $reactions_count AS Uint32;
    DECLARE $is_repost AS Bool;
    DECLARE $is_pinned AS Bool;
    DECLARE $is_edited AS Bool;
    DECLARE $is_deleted AS Bool;
    DECLARE $visibility AS Utf8;
    DECLARE $original_post_id AS Utf8;
    DECLARE $repost_comment AS Utf8;

    UPSERT INTO {POSTS_TABLE} (
        post_id, user_id, content, content_preview, media_urls, reactions,
        created_at, updated_at,
        comments_count, reposts_count, views_count, bookmarks_count, reactions_count,
        is_repost, is_pinned, is_edited, is_deleted, visibility,
        original_post_id, repost_comment
    ) VALUES (
        $post_id, $user_id, $content, $content_preview, $media_urls, $reactions,
        $created_at, $updated_at,
        $comments_count, $reposts_count, $views_count, $bookmarks_count, $reactions_count,
        $is_repost, $is_pinned, $is_edited, $is_deleted, $visibility,
        $original_post_id, $repost_comment
    );
    """
    prepared = session.prepare(query)
    session.transaction().execute(
        prepared,
        {
            '$post_id': post_id,
            '$user_id': str(user_id),
            '$content': comment,
            '$content_preview': preview,
            '$media_urls': '[]',
            '$reactions': '{}',
            '$created_at': created_at_ts,
            '$updated_at': created_at_ts,
            '$comments_count': 0,
            '$reposts_count': 0,
            '$views_count': 0,
            '$bookmarks_count': 0,
            '$reactions_count': 0,
            '$is_repost': True,
            '$is_pinned': False,
            '$is_edited': False,
            '$is_deleted': False,
            '$visibility': 'public',
            '$original_post_id': original_post_id,
            '$repost_comment': comment,
        },
        commit_tx=True,
    )
    return post_id


def run_migration():
    print("🚀 Starting reposts backfill migration...")
    ydb_pool.initialize()

    with ydb_pool.acquire() as session:
        reposts = get_all_reposts(session)
        print(f"📊 Found {len(reposts)} reposts in feed_reposts")

        created = 0
        skipped = 0
        errors = 0

        for row in reposts:
            repost_id = row.get('repost_id', '')
            original_post_id = row.get('original_post_id', '')
            user_id = str(row.get('user_id', ''))
            comment = row.get('comment', '') or ''
            created_at = row.get('created_at')

            if not original_post_id or not user_id:
                continue

            if isinstance(created_at, datetime):
                ts = int(created_at.timestamp() * 1_000_000)
            elif isinstance(created_at, int):
                ts = created_at
            else:
                ts = to_ts(datetime.utcnow())

            try:
                if repost_feed_post_exists(session, user_id, original_post_id):
                    skipped += 1
                    continue

                post_id = create_feed_post_for_repost(
                    session, user_id, original_post_id, comment, ts
                )
                created += 1
                print(f"  ✅ repost {repost_id[:8]}... → feed_post {post_id[:8]}...")
            except Exception as e:
                errors += 1
                print(f"  ⚠️  repost {repost_id[:8]}... → {e}")

        print()
        print("=" * 50)
        print(f"✅ Migration complete!")
        print(f"   feed_posts created : {created}")
        print(f"   already existed    : {skipped}")
        print(f"   errors             : {errors}")
        print("=" * 50)


if __name__ == "__main__":
    run_migration()
