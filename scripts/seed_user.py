"""Idempotent single-user seed: v1 is single-user, so this ensures exactly one User row
exists — never more, regardless of how many times it's run.

Safe to run repeatedly, same pattern as scripts/setup_sqs.py and scripts/setup_sns.py:
checks for an existing row first rather than blindly inserting.

Usage: python -m scripts.seed_user
"""

import asyncio
import logging

from sqlalchemy import select

from config.settings import settings
from db.client import async_session_factory
from db.models import User

logger = logging.getLogger(__name__)


async def ensure_user() -> User:
    async with async_session_factory() as session:
        existing = await session.scalar(select(User))
        if existing is not None:
            logger.info("user already seeded: %s (%s)", existing.id, existing.contact)
            return existing

        user = User(contact=settings.alert_email)
        session.add(user)
        await session.commit()
        await session.refresh(user)
        logger.info("seeded user: %s (%s)", user.id, user.contact)
        return user


async def main() -> None:
    logging.basicConfig(level=logging.INFO)
    await ensure_user()


if __name__ == "__main__":
    asyncio.run(main())
