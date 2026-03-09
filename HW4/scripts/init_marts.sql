DROP TABLE IF EXISTS mart_user_activity CASCADE;
DROP TABLE IF EXISTS mart_support_efficiency CASCADE;

CREATE TABLE mart_user_activity (
    user_id VARCHAR(50) PRIMARY KEY,
    total_sessions INTEGER,
    total_session_duration_minutes INTEGER,
    avg_session_duration_minutes NUMERIC(10,2),
    total_pages_visited INTEGER,
    avg_pages_per_session NUMERIC(10,2),
    total_actions INTEGER,
    avg_actions_per_session NUMERIC(10,2),
    most_used_device VARCHAR(20),
    first_session_date TIMESTAMP,
    last_session_date TIMESTAMP,
    days_active INTEGER,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE mart_support_efficiency (
    ticket_id VARCHAR(50) PRIMARY KEY,
    user_id VARCHAR(50),
    issue_type VARCHAR(50),
    status VARCHAR(20),
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    resolution_time_hours NUMERIC(10,2),
    messages_count INTEGER,
    is_resolved BOOLEAN,
    updated_at_mart TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_mart_user_activity_total_sessions ON mart_user_activity(total_sessions DESC);
CREATE INDEX idx_mart_user_activity_last_session ON mart_user_activity(last_session_date DESC);
CREATE INDEX idx_mart_support_status ON mart_support_efficiency(status);
CREATE INDEX idx_mart_support_issue_type ON mart_support_efficiency(issue_type);
