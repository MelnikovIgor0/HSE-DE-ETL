DROP TABLE IF EXISTS user_sessions CASCADE;
DROP TABLE IF EXISTS event_logs CASCADE;
DROP TABLE IF EXISTS support_tickets CASCADE;
DROP TABLE IF EXISTS support_messages CASCADE;
DROP TABLE IF EXISTS user_recommendations CASCADE;
DROP TABLE IF EXISTS moderation_queue CASCADE;

CREATE TABLE user_sessions (
    id SERIAL PRIMARY KEY,
    session_id VARCHAR(50) UNIQUE NOT NULL,
    user_id VARCHAR(50) NOT NULL,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP NOT NULL,
    device VARCHAR(20),
    pages_visited TEXT[],
    actions TEXT[],
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE event_logs (
    id SERIAL PRIMARY KEY,
    event_id VARCHAR(50) UNIQUE NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    event_type VARCHAR(50),
    page VARCHAR(200),
    element VARCHAR(100),
    user_id VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE support_tickets (
    id SERIAL PRIMARY KEY,
    ticket_id VARCHAR(50) UNIQUE NOT NULL,
    user_id VARCHAR(50) NOT NULL,
    issue_type VARCHAR(50),
    status VARCHAR(20),
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    messages_count INTEGER DEFAULT 0,
    created_at_db TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE support_messages (
    id SERIAL PRIMARY KEY,
    ticket_id VARCHAR(50) NOT NULL,
    sender VARCHAR(20),
    message TEXT,
    timestamp TIMESTAMP,
    FOREIGN KEY (ticket_id) REFERENCES support_tickets(ticket_id) ON DELETE CASCADE
);

CREATE TABLE user_recommendations (
    id SERIAL PRIMARY KEY,
    user_id VARCHAR(50) UNIQUE NOT NULL,
    recommended_products TEXT[],
    last_updated TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE moderation_queue (
    id SERIAL PRIMARY KEY,
    review_id VARCHAR(50) UNIQUE NOT NULL,
    product_id VARCHAR(50),
    user_id VARCHAR(50),
    review_text TEXT,
    rating INTEGER CHECK (rating BETWEEN 1 AND 5),
    flags TEXT[],
    moderation_status VARCHAR(20),
    submitted_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_user_sessions_user_id ON user_sessions(user_id);
CREATE INDEX idx_user_sessions_start_time ON user_sessions(start_time);
CREATE INDEX idx_event_logs_timestamp ON event_logs(timestamp);
CREATE INDEX idx_event_logs_event_type ON event_logs(event_type);
CREATE INDEX idx_support_tickets_status ON support_tickets(status);
CREATE INDEX idx_support_tickets_issue_type ON support_tickets(issue_type);
CREATE INDEX idx_support_messages_ticket_id ON support_messages(ticket_id);
CREATE INDEX idx_user_recommendations_user_id ON user_recommendations(user_id);
CREATE INDEX idx_moderation_queue_status ON moderation_queue(moderation_status);
CREATE INDEX idx_moderation_queue_product_id ON moderation_queue(product_id);