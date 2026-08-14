"""A rule firing repeatedly during a sustained move should produce one delivered alert
and N recorded (not silently dropped) suppressions, with the cooldown window sliding
from the most recent fire — delivered or suppressed — not just the first delivered one.
"""

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from sqlalchemy import select

from db.models import AlertRule, Notification, OutboxEvent, Pair, User
from rules.evaluator import STATUS_PENDING, STATUS_SUPPRESSED_COOLDOWN, evaluate_rules_for_pair

COOLDOWN_SECONDS = 300


async def _make_pair_and_cooldown_rule(session) -> tuple[Pair, AlertRule]:
    user = User(contact="test@example.com")
    pair = Pair(
        kraken_pair_name="TESTCOOLDOWNUSD",
        display_name="TEST/USD",
        base_currency="TEST",
        quote_currency="USD",
        poll_interval_seconds=60,
        is_active=True,
    )
    session.add_all([user, pair])
    await session.flush()

    rule = AlertRule(
        user_id=user.id,
        pair_id=pair.id,
        rule_type="absolute_below",
        threshold=Decimal("1000"),
        cooldown_seconds=COOLDOWN_SECONDS,
        is_enabled=True,
    )
    session.add(rule)
    await session.flush()
    return pair, rule


async def test_sustained_move_delivers_once_and_suppresses_the_rest(db_session) -> None:
    pair, rule = await _make_pair_and_cooldown_rule(db_session)
    base = datetime(2026, 8, 15, 9, 0, 0, tzinfo=UTC)

    # A sustained drop: each price is different (so idempotency doesn't dedup them —
    # this is genuinely new detected state each time) but all qualify and land within
    # the cooldown window of the first fire.
    prices = [Decimal("900"), Decimal("890"), Decimal("880"), Decimal("870"), Decimal("860")]
    offsets = [0, 30, 60, 90, 120]

    fired: list[Notification] = []
    for price, offset in zip(prices, offsets, strict=True):
        observed_at = base + timedelta(seconds=offset)
        created = await evaluate_rules_for_pair(db_session, pair, price, observed_at)
        assert len(created) == 1
        fired.append(created[0])

    assert fired[0].status == STATUS_PENDING
    assert all(n.status == STATUS_SUPPRESSED_COOLDOWN for n in fired[1:])

    # last_fired_at slid to the most recent fire (delivered or suppressed), not frozen
    # at the first delivery. Checked on the live ORM object directly, not via
    # session.refresh() — refresh() re-fetches from the DB without autoflushing this
    # object's own pending change first, so it would read back the second-to-last value.
    assert rule.last_fired_at == base + timedelta(seconds=120)

    delivered_outbox = (
        (await db_session.execute(select(OutboxEvent).where(OutboxEvent.notification_id == fired[0].id)))
        .scalars()
        .all()
    )
    assert len(delivered_outbox) == 1

    for suppressed_notification in fired[1:]:
        outbox = (
            (
                await db_session.execute(
                    select(OutboxEvent).where(OutboxEvent.notification_id == suppressed_notification.id)
                )
            )
            .scalars()
            .all()
        )
        assert outbox == [], "a suppressed fire must not get an OutboxEvent — it's recorded, not delivered"

    # Past the cooldown window measured from the LAST fire (t0+120s), a new qualifying
    # move should deliver again.
    past_cooldown = base + timedelta(seconds=120 + COOLDOWN_SECONDS + 1)
    resumed = await evaluate_rules_for_pair(db_session, pair, Decimal("850"), past_cooldown)
    assert len(resumed) == 1
    assert resumed[0].status == STATUS_PENDING

    all_notifications = (
        (await db_session.execute(select(Notification).where(Notification.pair_id == pair.id))).scalars().all()
    )
    assert len(all_notifications) == 6
    assert sum(1 for n in all_notifications if n.status == STATUS_PENDING) == 2
    assert sum(1 for n in all_notifications if n.status == STATUS_SUPPRESSED_COOLDOWN) == 4


async def test_first_ever_fire_is_never_suppressed(db_session) -> None:
    pair, _rule = await _make_pair_and_cooldown_rule(db_session)
    created = await evaluate_rules_for_pair(db_session, pair, Decimal("900"), datetime(2026, 8, 15, 9, 0, 0, tzinfo=UTC))
    assert len(created) == 1
    assert created[0].status == STATUS_PENDING


async def test_zero_cooldown_never_suppresses(db_session) -> None:
    user = User(contact="test@example.com")
    pair = Pair(
        kraken_pair_name="TESTNOCOOLDOWNUSD",
        display_name="TEST/USD",
        base_currency="TEST",
        quote_currency="USD",
        poll_interval_seconds=60,
        is_active=True,
    )
    db_session.add_all([user, pair])
    await db_session.flush()

    rule = AlertRule(
        user_id=user.id,
        pair_id=pair.id,
        rule_type="absolute_below",
        threshold=Decimal("1000"),
        cooldown_seconds=0,
        is_enabled=True,
    )
    db_session.add(rule)
    await db_session.flush()

    base = datetime(2026, 8, 15, 9, 0, 0, tzinfo=UTC)
    first = await evaluate_rules_for_pair(db_session, pair, Decimal("900"), base)
    second = await evaluate_rules_for_pair(db_session, pair, Decimal("899"), base + timedelta(seconds=1))

    assert first[0].status == STATUS_PENDING
    assert second[0].status == STATUS_PENDING
