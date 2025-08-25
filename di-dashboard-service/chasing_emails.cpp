"""
Chasing Email Module for HR Data Remediation
Sends follow-up emails to users who haven't taken action on their HR data after a week.
"""

import logging
import time
from sqlalchemy import create_engine, text
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.header import Header
import smtplib
import config

logger = logging.getLogger(__name__)


def get_users_for_chasing_emails():
    """
    Query users who need chasing emails based on:
    - Received HR data notifications over a week ago
    - Still have unremediated data (no actions taken)
    - Have valid email addresses
    
    Returns:
        list: List of dictionaries containing user email and related data
    """
    connection_string = config.mssql_conn
    engine = create_engine(connection_string)
    
    query = text("""
        SELECT DISTINCT 
            fo.OwnerEmail,
            COALESCE(fo.OwnerName, 'User') as OwnerName,
            COUNT(DISTINCT fop.ID) as PendingFiles,
            MAX(last_ok.NotificationDate) as LastNotificationDate,
            COALESCE(MAX(chase_count.ChasingCount), 0) as TotalChasingCount
        FROM FileOwner fo
        JOIN FileOwnership fop ON fop.PSID = fo.PSID
        JOIN DIRaw didat ON didat.Id = fop.FileID
        OUTER APPLY (
            SELECT TOP (1) un.NotificationDate
            FROM UserNotification un
            WHERE un.OwnershipId = fop.ID
              AND un.NotificationType = 'm'
              AND un.Finished = 1
              AND un.IsError = 0
            ORDER BY un.NotificationDate DESC
        ) last_ok
        OUTER APPLY (
            SELECT MAX(un.ChasingCount) as ChasingCount
            FROM UserNotification un
            WHERE un.OwnershipId = fop.ID
              AND un.NotificationType = 'c'
              AND un.Finished = 1
              AND un.IsError = 0
        ) chase_count
        WHERE last_ok.NotificationDate IS NOT NULL
          AND last_ok.NotificationDate <= DATEADD(day, -7, GETUTCDATE())
          AND NOT EXISTS (SELECT 1 FROM LabelAction la WHERE la.OwnershipId = fop.ID)
          AND NOT EXISTS (SELECT 1 FROM DeleteAction da WHERE da.OwnershipId = fop.ID)
          AND NOT EXISTS (SELECT 1 FROM UserAction ua WHERE ua.OwnershipId = fop.ID)
          AND didat.Load_For = 'HR'
          AND NULLIF(LTRIM(RTRIM(fo.OwnerEmail)), '') IS NOT NULL
        GROUP BY fo.OwnerEmail, fo.OwnerName
        ORDER BY fo.OwnerEmail
    """)
    
    try:
        with engine.connect() as connection:
            result = connection.execute(query)
            users = []
            for row in result:
                users.append({
                    'email': row.OwnerEmail,
                    'name': row.OwnerName,
                    'pending_files': row.PendingFiles,
                    'last_notification_date': row.LastNotificationDate,
                    'total_chasing_count': row.TotalChasingCount
                })
            logger.info(f"Found {len(users)} users requiring chasing emails")
            return users
    except Exception as e:
        logger.error(f"Database error while fetching users for chasing emails: {str(e)}")
        raise



def create_chasing_email_message(user_data, template):
    """
    Create the complete email message for chasing email
    
    Args:
        user_data (dict): User information including name, email, pending files count
        template (str): Template name/path for rendering
        
    Returns:
        MIMEMultipart: Complete email message ready to send
    """
    # Create subject - consistent with existing system
    title = "Data Security Warning Notification"
    
    # Render template with user data
    html = render_template(template, user_data=user_data)
    
    # Message preparation
    message = MIMEMultipart()
    html_part = MIMEText(html, "html", "utf-8")
    message.attach(html_part)
    message["Subject"] = Header(title, 'utf-8')
    message.add_header("X-ICCategory", "2")
    
    return message


def render_template(template, **kwargs):
    """
    Placeholder for template rendering - to be implemented with actual template engine
    
    Args:
        template (str): Template name/path
        **kwargs: Template variables
        
    Returns:
        str: Rendered HTML content
    """
    # TODO: Implement with actual template engine (Jinja2, etc.)
    # For now, return a basic template structure
    user_data = kwargs.get('user_data', {})
    user_name = user_data.get('name', 'User')
    pending_files = user_data.get('pending_files', 0)
    chase_count = user_data.get('total_chasing_count', 0)
    
    # This will be replaced with actual template rendering
    return f"""
    <html>
    <body>
        <h2>Data Security Warning Notification</h2>
        <p>Dear {user_name},</p>
        
        <p>This is a reminder regarding HR data files that require your attention.</p>
        
        <p><strong>Summary:</strong></p>
        <ul>
            <li>You have <strong>{pending_files}</strong> file(s) that still need remediation</li>
            <li>These files were flagged over a week ago</li>
            <li>No action has been taken on these files yet</li>
        </ul>
        
        <p><strong>Required Actions:</strong></p>
        <p>Please log into the data dashboard and take appropriate action.</p>
        
        <p>Best regards,<br>
        Data Governance Team</p>
    </body>
    </html>
    """


def send_mail(torecipients, message, ccrecipients=None, bccrecipients=None, max_retries=3):
    """
    Generic email sending function with retry logic
    
    Args:
        torecipients (list): List of primary recipients
        message (MIMEMultipart): Prepared email message
        ccrecipients (list): List of CC recipients
        bccrecipients (list): List of BCC recipients
        max_retries (int): Maximum number of retry attempts
        
    Returns:
        tuple: (successful_recipients, failed_recipients)
    """
    if ccrecipients is None:
        ccrecipients = []
    if bccrecipients is None:
        bccrecipients = []
        
    if not isinstance(torecipients, list):
        torecipients = [torecipients]
    
    successful_recipients = []
    failed_recipients = []
    
    server = None
    try:
        server = smtplib.SMTP(config.smtp_host, config.smtp_port, timeout=10)
        server.set_debuglevel(1)
        server.ehlo()
        server.starttls()
        server.ehlo()
        
        for recipient in torecipients:
            retry_count = 0
            sent_successfully = False
            
            while retry_count < max_retries and not sent_successfully:
                try:
                    server.sendmail(config.smtp_sender, recipient, message.as_string())
                    logger.info(f"Email sent successfully to: {recipient}")
                    successful_recipients.append(recipient)
                    sent_successfully = True
                    
                except smtplib.SMTPException as e:
                    retry_count += 1
                    logger.warning(f"SMTP error sending to {recipient} (attempt {retry_count}/{max_retries}): {e}")
                    if retry_count < max_retries:
                        time.sleep(2 ** retry_count)  # Exponential backoff
                    else:
                        logger.error(f"Failed to send email to {recipient} after {max_retries} attempts")
                        failed_recipients.append(recipient)
                        
                except Exception as e:
                    retry_count += 1
                    logger.warning(f"Unexpected error sending to {recipient} (attempt {retry_count}/{max_retries}): {e}")
                    if retry_count < max_retries:
                        time.sleep(2 ** retry_count)  # Exponential backoff
                    else:
                        logger.error(f"Failed to send email to {recipient} after {max_retries} attempts")
                        failed_recipients.append(recipient)
                        
    except Exception as e:
        logger.error(f"Failed to establish SMTP connection: {e}")
        failed_recipients.extend(torecipients)
        
    finally:
        if server:
            try:
                server.quit()
            except Exception as e:
                logger.warning(f"Error closing SMTP connection: {e}")
    
    return successful_recipients, failed_recipients


def update_chasing_email_notifications(successful_emails, failed_emails, user_data_map):
    """
    Update UserNotification table after sending chasing emails
    
    Args:
        successful_emails (list): List of emails that were sent successfully
        failed_emails (list): List of emails that failed to send
        user_data_map (dict): Map of email -> user_data for each recipient
    """
    connection_string = config.mssql_conn
    engine = create_engine(connection_string)
    
    total_updated = 0
    
    try:
        with engine.connect() as connection:
            # Process successful emails
            for email in successful_emails:
                user_data = user_data_map.get(email, {})
                current_chase_count = user_data.get('total_chasing_count', 0)
                next_chase_count = current_chase_count + 1
                
                query = text("""
                    INSERT INTO UserNotification(OwnershipId, NotificationDate, NotificationType, Finished, IsError, ChasingCount)
                    SELECT fop.ID, SYSDATETIME(), 'c', 1, 0, :chasing_count
                    FROM FileOwnership fop
                    JOIN FileOwner fo ON fo.PSID = fop.PSID
                    JOIN DIRaw didat ON didat.Id = fop.FileID
                    WHERE fo.OwnerEmail = :email
                      AND didat.Load_For = 'HR'
                      AND NOT EXISTS (
                          SELECT 1 FROM UserNotification un 
                          WHERE un.OwnershipId = fop.ID 
                            AND un.NotificationType = 'c'
                            AND un.NotificationDate >= DATEADD(day, -1, SYSDATETIME())
                      )
                """)
                
                result = connection.execute(query, {
                    "email": email,
                    "chasing_count": next_chase_count
                })
                total_updated += result.rowcount
                logger.info(f"Updated {result.rowcount} notification records for successful chasing email #{next_chase_count} to {email}")
            
            # Process failed emails
            for email in failed_emails:
                user_data = user_data_map.get(email, {})
                current_chase_count = user_data.get('total_chasing_count', 0)
                next_chase_count = current_chase_count + 1
                
                query = text("""
                    INSERT INTO UserNotification(OwnershipId, NotificationDate, NotificationType, Finished, IsError, ChasingCount)
                    SELECT fop.ID, SYSDATETIME(), 'c', 1, 1, :chasing_count
                    FROM FileOwnership fop
                    JOIN FileOwner fo ON fo.PSID = fop.PSID
                    JOIN DIRaw didat ON didat.Id = fop.FileID
                    WHERE fo.OwnerEmail = :email
                      AND didat.Load_For = 'HR'
                      AND NOT EXISTS (
                          SELECT 1 FROM UserNotification un 
                          WHERE un.OwnershipId = fop.ID 
                            AND un.NotificationType = 'c'
                            AND un.NotificationDate >= DATEADD(day, -1, SYSDATETIME())
                      )
                """)
                
                result = connection.execute(query, {
                    "email": email,
                    "chasing_count": next_chase_count
                })
                total_updated += result.rowcount
                logger.info(f"Updated {result.rowcount} notification records for failed chasing email #{next_chase_count} to {email}")
            
            connection.commit()
            logger.info(f"Total notification records updated: {total_updated}")
            return total_updated
            
    except Exception as e:
        logger.error(f"Database error updating chasing email notifications: {str(e)}")
        raise


def start_chasing_emails_send():
    """
    Main function to start the chasing email process
    """
    connection_string = config.mssql_conn
    engine = create_engine(connection_string)
    
    try:
        with engine.connect() as connection:
            if config.env == 'PROD':
                # Get users who need chasing emails
                users_to_chase = get_users_for_chasing_emails()
                
                if not users_to_chase:
                    logger.info("No users found requiring chasing emails")
                    return
                
                logger.info(f"Starting chasing email process for {len(users_to_chase)} users")
                
                # Prepare email data
                all_recipients = []
                user_data_map = {}
                
                for user_data in users_to_chase:
                    try:
                        # Create complete email message
                        message = create_chasing_email_message(user_data, template='chasing_email_template')
                        
                        # Send chasing email
                        successful, failed = send_mail(
                            torecipients=[user_data['email']],
                            message=message,
                            ccrecipients=[],
                            bccrecipients=[]
                        )
                        
                        # Track results for database updates
                        user_data_map[user_data['email']] = user_data
                        all_recipients.extend(successful)
                        all_recipients.extend(failed)
                        
                        # Update notifications
                        update_chasing_email_notifications(successful, failed, {user_data['email']: user_data})
                        
                    except Exception as e:
                        logger.error(f"Error processing chasing email for {user_data['email']}: {str(e)}")
                        continue
                
                logger.info("Chasing email process completed")
            else:
                logger.info("Chasing emails skipped - not in PROD environment")
                
    except Exception as e:
        logger.error(f"Database error in chasing email process: {str(e)}")
        raise


def get_chasing_emails_to_send():
    """
    Get count of users who would receive chasing emails (for monitoring/reporting)
    
    Returns:
        int: Number of users who would receive chasing emails
    """
    try:
        users = get_users_for_chasing_emails()
        return len(users)
    except Exception as e:
        logger.error(f"Error getting chasing email count: {str(e)}")
        return 0


if __name__ == "__main__":
    # For testing purposes
    logging.basicConfig(level=logging.INFO)
    start_chasing_emails_send()

/*
DECLARE @TestPSIDs TABLE (PSID varchar(20), TestScenario varchar(50));
INSERT INTO @TestPSIDs VALUES 
    ('TEST_PSID_001', 'Normal_8_days_old'),
    ('TEST_PSID_002', 'Exactly_7_days_old'), 
    ('TEST_PSID_003', 'Multiple_files_same_user'),
    ('TEST_PSID_004', 'Duplicate_email_test'),
    ('TEST_PSID_005', 'Failed_notification_test'),
    ('TEST_PSID_006', 'User_with_actions_excluded'),
    ('TEST_PSID_007', 'Edge_case_6_days_old');

-- STEP 2A: Normal case - 8 days old (should trigger chasing)
INSERT INTO UserNotification (OwnershipId, NotificationDate, NotificationType, Finished, IsError, ChasingCount)
SELECT 
    fop.ID as OwnershipId,
    DATEADD(day, -8, GETUTCDATE()) as NotificationDate,
    'm' as NotificationType,
    1 as Finished,
    0 as IsError,
    0 as ChasingCount
FROM FileOwnership fop
JOIN @TestPSIDs tp ON fop.PSID = tp.PSID
JOIN DIRaw didat ON didat.Id = fop.FileID
WHERE didat.Load_For = 'HR'
  AND tp.TestScenario = 'Normal_8_days_old'
  AND NOT EXISTS (
      SELECT 1 FROM UserNotification un 
      WHERE un.OwnershipId = fop.ID 
        AND un.NotificationType = 'm'
  );

-- STEP 2B: Edge case - exactly 7 days old (should trigger chasing)
INSERT INTO UserNotification (OwnershipId, NotificationDate, NotificationType, Finished, IsError, ChasingCount)
SELECT 
    fop.ID as OwnershipId,
    DATEADD(day, -7, GETUTCDATE()) as NotificationDate,
    'm' as NotificationType,
    1 as Finished,
    0 as IsError,
    0 as ChasingCount
FROM FileOwnership fop
JOIN @TestPSIDs tp ON fop.PSID = tp.PSID
JOIN DIRaw didat ON didat.Id = fop.FileID
WHERE didat.Load_For = 'HR'
  AND tp.TestScenario = 'Exactly_7_days_old'
  AND NOT EXISTS (
      SELECT 1 FROM UserNotification un 
      WHERE un.OwnershipId = fop.ID 
        AND un.NotificationType = 'm'
  );

-- STEP 2C: Edge case - 6 days old (should NOT trigger chasing)
INSERT INTO UserNotification (OwnershipId, NotificationDate, NotificationType, Finished, IsError, ChasingCount)
SELECT 
    fop.ID as OwnershipId,
    DATEADD(day, -6, GETUTCDATE()) as NotificationDate,
    'm' as NotificationType,
    1 as Finished,
    0 as IsError,
    0 as ChasingCount
FROM FileOwnership fop
JOIN @TestPSIDs tp ON fop.PSID = tp.PSID
JOIN DIRaw didat ON didat.Id = fop.FileID
WHERE didat.Load_For = 'HR'
  AND tp.TestScenario = 'Edge_case_6_days_old'
  AND NOT EXISTS (
      SELECT 1 FROM UserNotification un 
      WHERE un.OwnershipId = fop.ID 
        AND un.NotificationType = 'm'
  );

-- STEP 2D: Multiple files same user (test aggregation)
INSERT INTO UserNotification (OwnershipId, NotificationDate, NotificationType, Finished, IsError, ChasingCount)
SELECT 
    fop.ID as OwnershipId,
    DATEADD(day, -9, GETUTCDATE()) as NotificationDate,
    'm' as NotificationType,
    1 as Finished,
    0 as IsError,
    0 as ChasingCount
FROM FileOwnership fop
JOIN @TestPSIDs tp ON fop.PSID = tp.PSID
JOIN DIRaw didat ON didat.Id = fop.FileID
WHERE didat.Load_For = 'HR'
  AND tp.TestScenario = 'Multiple_files_same_user'
  AND NOT EXISTS (
      SELECT 1 FROM UserNotification un 
      WHERE un.OwnershipId = fop.ID 
        AND un.NotificationType = 'm'
  );

-- STEP 2E: Duplicate email test (same user, different files)
INSERT INTO UserNotification (OwnershipId, NotificationDate, NotificationType, Finished, IsError, ChasingCount)
SELECT 
    fop.ID as OwnershipId,
    DATEADD(day, -10, GETUTCDATE()) as NotificationDate,
    'm' as NotificationType,
    1 as Finished,
    0 as IsError,
    0 as ChasingCount
FROM FileOwnership fop
JOIN @TestPSIDs tp ON fop.PSID = tp.PSID
JOIN DIRaw didat ON didat.Id = fop.FileID
WHERE didat.Load_For = 'HR'
  AND tp.TestScenario = 'Duplicate_email_test'
  AND NOT EXISTS (
      SELECT 1 FROM UserNotification un 
      WHERE un.OwnershipId = fop.ID 
        AND un.NotificationType = 'm'
  );

-- STEP 2F: Failed notification test (IsError = 1, should still trigger chasing if Finished = 1)
INSERT INTO UserNotification (OwnershipId, NotificationDate, NotificationType, Finished, IsError, ChasingCount)
SELECT 
    fop.ID as OwnershipId,
    DATEADD(day, -8, GETUTCDATE()) as NotificationDate,
    'm' as NotificationType,
    1 as Finished,
    1 as IsError,  -- Failed notification
    0 as ChasingCount
FROM FileOwnership fop
JOIN @TestPSIDs tp ON fop.PSID = tp.PSID
JOIN DIRaw didat ON didat.Id = fop.FileID
WHERE didat.Load_For = 'HR'
  AND tp.TestScenario = 'Failed_notification_test'
  AND NOT EXISTS (
      SELECT 1 FROM UserNotification un 
      WHERE un.OwnershipId = fop.ID 
        AND un.NotificationType = 'm'
  );

-- STEP 3A: Add user actions to exclude some users from chasing
-- This tests that users who took actions are properly excluded
INSERT INTO LabelAction (OwnershipId, ActionDate, IsDeleted, IsLabeled, IsFake)
SELECT 
    fop.ID as OwnershipId,
    DATEADD(day, -2, GETUTCDATE()) as ActionDate,
    0 as IsDeleted,
    1 as IsLabeled,
    0 as IsFake
FROM FileOwnership fop
JOIN @TestPSIDs tp ON fop.PSID = tp.PSID
JOIN DIRaw didat ON didat.Id = fop.FileID
WHERE didat.Load_For = 'HR'
  AND tp.TestScenario = 'User_with_actions_excluded';

-- STEP 3B: Add existing chasing emails to test duplicate prevention
INSERT INTO UserNotification (OwnershipId, NotificationDate, NotificationType, Finished, IsError, ChasingCount)
SELECT 
    fop.ID as OwnershipId,
    DATEADD(hour, -12, GETUTCDATE()) as NotificationDate,  -- 12 hours ago (within 24h window)
    'c' as NotificationType,
    1 as Finished,
    0 as IsError,
    1 as ChasingCount
FROM FileOwnership fop
JOIN @TestPSIDs tp ON fop.PSID = tp.PSID
JOIN DIRaw didat ON didat.Id = fop.FileID
WHERE didat.Load_For = 'HR'
  AND tp.TestScenario = 'Duplicate_email_test';

-- STEP 3C: Add escalation test data (multiple chasing emails)
INSERT INTO UserNotification (OwnershipId, NotificationDate, NotificationType, Finished, IsError, ChasingCount)
SELECT 
    fop.ID as OwnershipId,
    DATEADD(day, -3, GETUTCDATE()) as NotificationDate,
    'c' as NotificationType,
    1 as Finished,
    0 as IsError,
    1 as ChasingCount
FROM FileOwnership fop
JOIN @TestPSIDs tp ON fop.PSID = tp.PSID
JOIN DIRaw didat ON didat.Id = fop.FileID
WHERE didat.Load_For = 'HR'
  AND tp.TestScenario = 'Multiple_files_same_user';

-- Add second chasing email for escalation testing
INSERT INTO UserNotification (OwnershipId, NotificationDate, NotificationType, Finished, IsError, ChasingCount)
SELECT 
    fop.ID as OwnershipId,
    DATEADD(day, -1, GETUTCDATE()) as NotificationDate,
    'c' as NotificationType,
    1 as Finished,
    0 as IsError,
    2 as ChasingCount
FROM FileOwnership fop
JOIN @TestPSIDs tp ON fop.PSID = tp.PSID
JOIN DIRaw didat ON didat.Id = fop.FileID
WHERE didat.Load_For = 'HR'
  AND tp.TestScenario = 'Multiple_files_same_user';

-- STEP 4: Comprehensive verification queries

-- 4A: Check all test notifications created
SELECT 
    tp.TestScenario,
    fo.OwnerEmail,
    fo.OwnerName,
    fop.PSID,
    un.NotificationType,
    un.NotificationDate,
    DATEDIFF(day, un.NotificationDate, GETUTCDATE()) as DaysAgo,
    un.ChasingCount,
    un.Finished,
    un.IsError,
    CASE 
        WHEN un.NotificationType = 'm' AND DATEDIFF(day, un.NotificationDate, GETUTCDATE()) >= 7 THEN 'Should trigger chasing'
        WHEN un.NotificationType = 'm' AND DATEDIFF(day, un.NotificationDate, GETUTCDATE()) < 7 THEN 'Should NOT trigger chasing'
        WHEN un.NotificationType = 'c' THEN 'Chasing email'
        ELSE 'Other'
    END as ExpectedBehavior
FROM UserNotification un
JOIN FileOwnership fop ON un.OwnershipId = fop.ID
JOIN FileOwner fo ON fop.PSID = fo.PSID
JOIN @TestPSIDs tp ON fop.PSID = tp.PSID
ORDER BY tp.TestScenario, fo.OwnerEmail, un.NotificationDate;

-- 4B: Check user actions that should exclude users
SELECT 
    tp.TestScenario,
    fo.OwnerEmail,
    'LabelAction' as ActionType,
    la.ActionDate,
    'Should be excluded from chasing' as ExpectedBehavior
FROM LabelAction la
JOIN FileOwnership fop ON la.OwnershipId = fop.ID
JOIN FileOwner fo ON fop.PSID = fo.PSID
JOIN @TestPSIDs tp ON fop.PSID = tp.PSID
WHERE tp.TestScenario = 'User_with_actions_excluded'

UNION ALL

SELECT 
    tp.TestScenario,
    fo.OwnerEmail,
    'DeleteAction' as ActionType,
    da.ActionDate,
    'Should be excluded from chasing' as ExpectedBehavior
FROM DeleteAction da
JOIN FileOwnership fop ON da.OwnershipId = fop.ID
JOIN FileOwner fo ON fop.PSID = fo.PSID
JOIN @TestPSIDs tp ON fop.PSID = tp.PSID
WHERE tp.TestScenario = 'User_with_actions_excluded'

UNION ALL

SELECT 
    tp.TestScenario,
    fo.OwnerEmail,
    'UserAction' as ActionType,
    ua.ActionDate,
    'Should be excluded from chasing' as ExpectedBehavior
FROM UserAction ua
JOIN FileOwnership fop ON ua.OwnershipId = fop.ID
JOIN FileOwner fo ON fop.PSID = fo.PSID
JOIN @TestPSIDs tp ON fop.PSID = tp.PSID
WHERE tp.TestScenario = 'User_with_actions_excluded'
ORDER BY TestScenario, OwnerEmail;

-- 4C: Test duplicate prevention (recent chasing emails within 24h)
SELECT 
    tp.TestScenario,
    fo.OwnerEmail,
    un.NotificationDate,
    DATEDIFF(hour, un.NotificationDate, GETUTCDATE()) as HoursAgo,
    CASE 
        WHEN DATEDIFF(hour, un.NotificationDate, GETUTCDATE()) < 24 THEN 'Should prevent duplicate'
        ELSE 'Allow new chasing email'
    END as DuplicatePreventionStatus
FROM UserNotification un
JOIN FileOwnership fop ON un.OwnershipId = fop.ID
JOIN FileOwner fo ON fop.PSID = fo.PSID
JOIN @TestPSIDs tp ON fop.PSID = tp.PSID
WHERE un.NotificationType = 'c'
  AND tp.TestScenario = 'Duplicate_email_test'
ORDER BY tp.TestScenario, fo.OwnerEmail;

-- STEP 5: Test the chasing email query with your test data
SELECT DISTINCT 
    fo.OwnerEmail,
    COALESCE(fo.OwnerName, 'User') as OwnerName,
    COUNT(DISTINCT fop.ID) as PendingFiles,
    MAX(last_ok.NotificationDate) as LastNotificationDate,
    COALESCE(MAX(chase_count.ChasingCount), 0) as TotalChasingCount
FROM FileOwner fo
JOIN FileOwnership fop ON fop.PSID = fo.PSID
JOIN DIRaw didat ON didat.Id = fop.FileID
JOIN @TestPSIDs tp ON fop.PSID = tp.PSID  -- Filter to test data only
OUTER APPLY (
    SELECT TOP (1) un.NotificationDate
    FROM UserNotification un
    WHERE un.OwnershipId = fop.ID
      AND un.NotificationType = 'm'
      AND un.Finished = 1
      AND un.IsError = 0
    ORDER BY un.NotificationDate DESC
) last_ok
OUTER APPLY (
    SELECT MAX(un.ChasingCount) as ChasingCount
    FROM UserNotification un
    WHERE un.OwnershipId = fop.ID
      AND un.NotificationType = 'c'
      AND un.Finished = 1
      AND un.IsError = 0
) chase_count
WHERE last_ok.NotificationDate IS NOT NULL
  AND last_ok.NotificationDate <= DATEADD(day, -7, GETUTCDATE())
  AND NOT EXISTS (SELECT 1 FROM LabelAction la WHERE la.OwnershipId = fop.ID)
  AND NOT EXISTS (SELECT 1 FROM DeleteAction da WHERE da.OwnershipId = fop.ID)
  AND NOT EXISTS (SELECT 1 FROM UserAction ua WHERE ua.OwnershipId = fop.ID)
  AND didat.Load_For = 'HR'
  AND NULLIF(LTRIM(RTRIM(fo.OwnerEmail)), '') IS NOT NULL
GROUP BY fo.OwnerEmail, fo.OwnerName
ORDER BY fo.OwnerEmail;

-- CLEANUP: Remove test data when done testing
-- UNCOMMENT WHEN YOU WANT TO CLEAN UP
/*
DELETE un 
FROM UserNotification un
JOIN FileOwnership fop ON un.OwnershipId = fop.ID
JOIN @TestPSIDs tp ON fop.PSID = tp.PSID
WHERE un.NotificationDate >= DATEADD(day, -10, GETUTCDATE());
*/

-- Add chasing email counter and manager escalation columns to UserNotification table
ALTER TABLE DIDashboard.dbo.UserNotification 
ADD ChasingCount int DEFAULT 0 NOT NULL;

ALTER TABLE DIDashboard.dbo.UserNotification 
ADD ManagerNotified bit DEFAULT 0 NOT NULL;

ALTER TABLE DIDashboard.dbo.UserNotification 
ADD ManagerNotificationDate datetime2 NULL;

-- Add index for efficient chasing email queries
CREATE NONCLUSTERED INDEX IX_UserNotification_ChasingLookup 
ON DIDashboard.dbo.UserNotification (OwnershipId, NotificationType, ChasingCount)
INCLUDE (NotificationDate, Finished, IsError);
*/