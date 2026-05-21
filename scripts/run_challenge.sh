#!/bin/bash
# =============================================================================
# Kafka Data Replication Challenge — Test Orchestration Script
#
# This script runs three test scenarios:
#   1. Normal replication flow (1000 messages)
#   2. Log truncation detection (fail-fast)
#   3. Graceful topic reset handling
#
# Usage: ./scripts/run_challenge.sh
# =============================================================================

set -euo pipefail

COMPOSE_FILE="docker-compose.yml"
PRIMARY_BOOTSTRAP="localhost:9092"
STANDBY_BOOTSTRAP="localhost:9094"
TOPIC="commit-log"
REPLICATED_TOPIC="primary.commit-log"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Assertion tracking
FAIL_COUNT=0
PASS_COUNT=0
SCENARIO_FAILED=0

log_info()  { echo -e "${CYAN}[INFO]${NC}  $(date '+%H:%M:%S') $*"; }
log_pass()  { echo -e "${GREEN}[PASS]${NC}  $(date '+%H:%M:%S') $*"; }
log_fail()  { echo -e "${RED}[FAIL]${NC}  $(date '+%H:%M:%S') $*"; }
log_warn()  { echo -e "${YELLOW}[WARN]${NC}  $(date '+%H:%M:%S') $*"; }
log_header() {
    echo ""
    echo -e "${CYAN}============================================================${NC}"
    echo -e "${CYAN}  $*${NC}"
    echo -e "${CYAN}============================================================${NC}"
    echo ""
}

# Assertion: test passes if condition is true
assert_pass() {
    local description="$1"
    local condition="$2"
    if eval "$condition"; then
        log_pass "$description"
        PASS_COUNT=$((PASS_COUNT + 1))
        return 0
    else
        log_fail "$description"
        FAIL_COUNT=$((FAIL_COUNT + 1))
        SCENARIO_FAILED=1
        return 1
    fi
}

# Assertion: test passes if value equals expected
assert_eq() {
    local description="$1"
    local actual="$2"
    local expected="$3"
    if [ "$actual" = "$expected" ]; then
        log_pass "$description (expected: $expected, got: $actual)"
        PASS_COUNT=$((PASS_COUNT + 1))
        return 0
    else
        log_fail "$description (expected: $expected, got: $actual)"
        FAIL_COUNT=$((FAIL_COUNT + 1))
        SCENARIO_FAILED=1
        return 1
    fi
}

# Assertion: test passes if actual >= expected
assert_ge() {
    local description="$1"
    local actual="$2"
    local expected="$3"
    if [ "$actual" -ge "$expected" ] 2>/dev/null; then
        log_pass "$description (expected: >=$expected, got: $actual)"
        PASS_COUNT=$((PASS_COUNT + 1))
        return 0
    else
        log_fail "$description (expected: >=$expected, got: $actual)"
        FAIL_COUNT=$((FAIL_COUNT + 1))
        SCENARIO_FAILED=1
        return 1
    fi
}

wait_for_kafka() {
    local bootstrap=$1
    local name=$2
    local max_wait=60
    local elapsed=0

    log_info "Waiting for $name to be ready at $bootstrap..."
    while ! docker compose -f "$COMPOSE_FILE" exec -T $([ "$bootstrap" = "$PRIMARY_BOOTSTRAP" ] && echo "primary-kafka" || echo "standby-kafka") \
        /opt/kafka/bin/kafka-cluster.sh cluster-id --bootstrap-server localhost:9092 > /dev/null 2>&1; do
        sleep 2
        elapsed=$((elapsed + 2))
        if [ $elapsed -ge $max_wait ]; then
            log_fail "$name did not become ready within ${max_wait}s"
            return 1
        fi
    done
    log_info "$name is ready."
}

get_topic_count() {
    local container=$1
    local topic=$2
    docker compose -f "$COMPOSE_FILE" exec -T "$container" \
        /opt/kafka/bin/kafka-get-offsets.sh \
        --bootstrap-server localhost:9092 --topic "$topic" --time latest 2>/dev/null | \
        awk -F: '{sum += $3} END {print sum+0}'
}

cleanup() {
    log_info "Cleaning up Docker environment..."
    docker compose -f "$COMPOSE_FILE" down -v --remove-orphans 2>/dev/null || true
}

# =============================================================================
# SCENARIO 1: Normal Replication Flow
# =============================================================================
scenario_normal_replication() {
    log_header "SCENARIO 1: Normal Replication Flow"

    log_info "Starting infrastructure..."
    docker compose -f "$COMPOSE_FILE" up -d primary-kafka standby-kafka
    sleep 10

    # Wait for clusters to be ready
    wait_for_kafka "$PRIMARY_BOOTSTRAP" "Primary Kafka"
    wait_for_kafka "$STANDBY_BOOTSTRAP" "Standby Kafka"

    # Create topic
    log_info "Creating commit-log topic..."
    docker compose -f "$COMPOSE_FILE" up init-topics
    sleep 5

    # Start MirrorMaker 2
    log_info "Starting MirrorMaker 2..."
    docker compose -f "$COMPOSE_FILE" up -d mirrormaker2
    sleep 15  # Allow MM2 to initialize and discover topics

    # Produce 1000 messages
    log_info "Producing 1000 messages to commit-log..."
    docker compose -f "$COMPOSE_FILE" run --rm commit-log-producer \
        --count 1000 --bootstrap-servers primary-kafka:9092 --topic commit-log

    # Wait for replication
    log_info "Waiting 30 seconds for replication to complete..."
    sleep 30

    # Verify message counts
    local primary_count=$(get_topic_count "primary-kafka" "$TOPIC")
    local standby_count=$(get_topic_count "standby-kafka" "$REPLICATED_TOPIC")

    log_info "Primary cluster (commit-log): $primary_count messages"
    log_info "Standby cluster (primary.commit-log): $standby_count messages"

    SCENARIO_FAILED=0
    assert_ge "All 1000 messages replicated to standby cluster" "$standby_count" 1000

    # Show MM2 logs
    log_info "MirrorMaker 2 recent logs:"
    docker compose -f "$COMPOSE_FILE" logs --tail=20 mirrormaker2

    cleanup
    return $SCENARIO_FAILED
}

# =============================================================================
# SCENARIO 2: Log Truncation Detection (Fail-Fast)
#
# Flow:
#   1. Produce 500 messages, start MM2 to replicate them (establishes offset tracking)
#   2. Stop MM2, wait for retention to delete messages
#   3. Verify truncation occurred (earliest offset on primary > 0)
#   4. Produce 100 more messages (new offsets beyond the gap)
#   5. Start MM2 again — it tries to resume from offset 500 which is gone
#   6. Enhanced MM2 detects truncation (earliestOffset > expectedOffset) and fails fast
# =============================================================================
scenario_truncation_detection() {
    log_header "SCENARIO 2: Log Truncation Detection (Fail-Fast)"

    log_info "Starting infrastructure..."
    docker compose -f "$COMPOSE_FILE" up -d primary-kafka standby-kafka
    sleep 10

    wait_for_kafka "$PRIMARY_BOOTSTRAP" "Primary Kafka"
    wait_for_kafka "$STANDBY_BOOTSTRAP" "Standby Kafka"

    # Create topic with 60s retention
    log_info "Creating commit-log topic with 60s retention..."
    docker compose -f "$COMPOSE_FILE" up init-topics
    sleep 5

    # Phase 1: Produce messages and let MM2 replicate (establishes offset tracking)
    log_info "Phase 1: Producing 500 messages..."
    docker compose -f "$COMPOSE_FILE" run --rm commit-log-producer \
        --count 500 --bootstrap-servers primary-kafka:9092 --topic commit-log

    log_info "Starting MirrorMaker 2 to replicate initial batch..."
    docker compose -f "$COMPOSE_FILE" up -d mirrormaker2
    sleep 30

    local phase1_count=$(get_topic_count "standby-kafka" "$REPLICATED_TOPIC")
    log_info "Phase 1 replication: $phase1_count messages on standby"

    # Stop MM2 to preserve its offset state
    log_info "Stopping MirrorMaker 2..."
    docker compose -f "$COMPOSE_FILE" stop mirrormaker2
    sleep 5

    # Phase 2: Wait for retention to delete the original 500 messages
    log_info "Phase 2: Waiting 90 seconds for log retention to truncate messages..."
    log_info "(retention.ms=60000, check interval=10000)"
    sleep 90

    # Verify truncation actually happened
    local earliest_offset=$(docker compose -f "$COMPOSE_FILE" exec -T primary-kafka \
        /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 \
        --topic commit-log --time earliest 2>/dev/null | awk -F: '{print $3}' | head -1)
    log_info "Earliest available offset on primary after retention: $earliest_offset"

    if [ "$earliest_offset" -gt 0 ] 2>/dev/null; then
        log_info "Truncation confirmed: earliest offset moved from 0 to $earliest_offset"
    else
        log_warn "Truncation may not have occurred yet (earliest offset still $earliest_offset)"
    fi

    # Produce more messages AFTER truncation (these will have higher offsets)
    log_info "Producing 100 more messages after truncation..."
    docker compose -f "$COMPOSE_FILE" run --rm commit-log-producer \
        --count 100 --bootstrap-servers primary-kafka:9092 --topic commit-log

    # Phase 3: Start MM2 again — it will try to resume from offset 500 which is gone
    log_info "Phase 3: Starting MirrorMaker 2 (should detect truncation)..."
    docker compose -f "$COMPOSE_FILE" up -d mirrormaker2
    sleep 30

    # Check MM2 logs for truncation detection
    log_info "Checking MirrorMaker 2 logs for truncation detection..."
    local mm2_logs=$(docker compose -f "$COMPOSE_FILE" logs mirrormaker2 2>&1)
    local truncation_detected=$(echo "$mm2_logs" | grep -c "LOG TRUNCATION DETECTED\|LogTruncationException" || true)

    # Also check if MM2 container failed (fail-fast behavior)
    local mm2_state=$(docker compose -f "$COMPOSE_FILE" ps mirrormaker2 --format json 2>/dev/null | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('State','unknown'))" 2>/dev/null || echo "unknown")
    log_info "MM2 container state: $mm2_state"

    SCENARIO_FAILED=0

    # Assertion 1: Truncation actually occurred (earliest offset > 0)
    assert_pass "Log truncation occurred on primary (earliest offset > 0)" \
        "[ \"$earliest_offset\" -gt 0 ] 2>/dev/null"

    # Assertion 2: MM2 detected truncation via log message or exception
    if [ "$truncation_detected" -gt 0 ]; then
        assert_pass "MM2 logged truncation detection (LOG TRUNCATION DETECTED or LogTruncationException)" \
            "[ \"$truncation_detected\" -gt 0 ]"
    elif [ "$mm2_state" = "exited" ] || [ "$mm2_state" = "restarting" ]; then
        assert_pass "MM2 failed fast on truncation (container state: $mm2_state)" \
            "[ \"$mm2_state\" = \"exited\" ] || [ \"$mm2_state\" = \"restarting\" ]"
    else
        assert_pass "MM2 detected truncation" "false"
    fi

    log_info "MirrorMaker 2 logs (last 40 lines):"
    docker compose -f "$COMPOSE_FILE" logs --tail=40 mirrormaker2

    cleanup
    return $SCENARIO_FAILED
}

# =============================================================================
# SCENARIO 3: Graceful Topic Reset Handling
#
# Flow:
#   1. Produce 200 messages, start MM2 to replicate (establishes offset tracking)
#   2. Stop MM2, delete and recreate the source topic
#   3. Produce 300 new messages to the recreated topic
#   4. Start MM2 again — it tries to resume from offset 200 which doesn't exist
#   5. Enhanced MM2 detects topic reset (earliestOffset == 0), seeks to beginning
#   6. Verify new messages are replicated
# =============================================================================
scenario_topic_reset() {
    log_header "SCENARIO 3: Graceful Topic Reset Handling"

    log_info "Starting infrastructure..."
    docker compose -f "$COMPOSE_FILE" up -d primary-kafka standby-kafka
    sleep 10

    wait_for_kafka "$PRIMARY_BOOTSTRAP" "Primary Kafka"
    wait_for_kafka "$STANDBY_BOOTSTRAP" "Standby Kafka"

    # Create topic and produce initial messages
    log_info "Creating commit-log topic..."
    docker compose -f "$COMPOSE_FILE" up init-topics
    sleep 5

    # Phase 1: Produce and replicate initial batch
    log_info "Phase 1: Producing 200 initial messages..."
    docker compose -f "$COMPOSE_FILE" run --rm commit-log-producer \
        --count 200 --bootstrap-servers primary-kafka:9092 --topic commit-log

    log_info "Starting MirrorMaker 2 to replicate initial batch..."
    docker compose -f "$COMPOSE_FILE" up -d mirrormaker2
    sleep 25

    local pre_reset_count=$(get_topic_count "standby-kafka" "$REPLICATED_TOPIC")
    log_info "Pre-reset: $pre_reset_count messages replicated to standby"

    # Phase 2: Stop MM2, delete and recreate topic
    log_info "Stopping MirrorMaker 2..."
    docker compose -f "$COMPOSE_FILE" stop mirrormaker2
    sleep 5

    log_info "Deleting commit-log topic..."
    docker compose -f "$COMPOSE_FILE" exec -T primary-kafka \
        /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
        --delete --topic commit-log
    sleep 5

    log_info "Recreating commit-log topic..."
    docker compose -f "$COMPOSE_FILE" exec -T primary-kafka \
        /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
        --create --topic commit-log --partitions 1 --replication-factor 1 \
        --config retention.ms=60000
    sleep 5

    # Phase 3: Produce new messages to recreated topic
    log_info "Phase 3: Producing 300 new messages to recreated topic..."
    docker compose -f "$COMPOSE_FILE" run --rm commit-log-producer \
        --count 300 --bootstrap-servers primary-kafka:9092 --topic commit-log

    # Phase 4: Start MM2 — should detect topic reset and recover
    log_info "Phase 4: Starting MirrorMaker 2 (should detect topic reset and recover)..."
    docker compose -f "$COMPOSE_FILE" up -d mirrormaker2
    sleep 30

    # Check for reset detection in logs
    local mm2_logs=$(docker compose -f "$COMPOSE_FILE" logs mirrormaker2 2>&1)
    local reset_detected=$(echo "$mm2_logs" | grep -c "TOPIC RESET DETECTED\|Topic reset recovery successful\|resubscrib" || true)

    SCENARIO_FAILED=0

    # Assertion 1: MM2 detected topic reset
    assert_pass "MM2 detected topic reset (TOPIC RESET DETECTED in logs)" \
        "[ \"$reset_detected\" -gt 0 ]"

    # Verify new messages are being replicated after recovery
    sleep 15
    local post_reset_count=$(get_topic_count "standby-kafka" "$REPLICATED_TOPIC")
    log_info "Post-reset: $post_reset_count total messages in standby cluster"

    # Assertion 2: Replication resumed after topic reset
    assert_pass "Replication resumed after topic reset ($pre_reset_count -> $post_reset_count messages)" \
        "[ \"$post_reset_count\" -gt \"$pre_reset_count\" ]"

    log_info "MirrorMaker 2 logs (last 40 lines):"
    docker compose -f "$COMPOSE_FILE" logs --tail=40 mirrormaker2

    cleanup
    return $SCENARIO_FAILED
}

# =============================================================================
# MAIN
# =============================================================================
main() {
    log_header "Kafka Data Replication Challenge — Test Suite"

    # Ensure clean state
    cleanup

    # Build producer image if needed
    log_info "Building commit-log-producer image..."
    docker compose -f "$COMPOSE_FILE" build commit-log-producer 2>/dev/null || true

    local overall_failed=0

    # Run all scenarios
    scenario_normal_replication || overall_failed=1
    scenario_truncation_detection || overall_failed=1
    scenario_topic_reset || overall_failed=1

    log_header "ALL SCENARIOS COMPLETE"
    echo ""
    echo -e "  Assertions: ${GREEN}${PASS_COUNT} passed${NC}, ${RED}${FAIL_COUNT} failed${NC}"
    echo ""

    if [ "$overall_failed" -eq 1 ] || [ "$FAIL_COUNT" -gt 0 ]; then
        log_fail "ONE OR MORE SCENARIOS FAILED. Check output above for details."
        exit 1
    else
        log_pass "ALL SCENARIOS PASSED SUCCESSFULLY."
        exit 0
    fi
}

main "$@"
