"""
Chasing Email Module for HR Data Remediation
Sends follow-up emails to users who haven't taken action on their HR data after a week.
"""

import logging
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



def send_chasing_email(torecipients, ccrecipients, bccrecipients, html, emailsubject, user_data):
    """
    Send chasing email using SMTP with proper notification tracking
    
    Args:
        torecipients (list): List of primary recipients
        ccrecipients (list): List of CC recipients  
        bccrecipients (list): List of BCC recipients
        html (str): HTML content of email
        emailsubject (str): Email subject
        user_data (dict): User data for tracking
    """
    # Message preparation
    message = MIMEMultipart()
    html_part = MIMEText(html, "html", "utf-8")
    message.attach(html_part)
    subject = emailsubject
    message["Subject"] = Header(subject, 'utf-8')
    
    if not isinstance(torecipients, list):
        torecipients = [torecipients]
    
    message.add_header("X-ICCategory", "2")
    server = smtplib.SMTP(config.smtp_host, config.smtp_port, timeout=10)
    server.set_debuglevel(1)
    server.ehlo()
    server.starttls()
    server.ehlo()
    
    for recipient in torecipients:
        is_okay = True
        try:
            server.connect(config.smtp_host, config.smtp_port)
            server.sendmail(config.smtp_sender, recipient, message.as_string())
            logger.warning(f"Chasing email sent to: {recipient}")
            is_okay = True
        except smtplib.SMTPException as e:
            is_okay = False
            logger.error(f"Failed to send chasing email to {recipient}: {e}")
        except Exception as e:
            is_okay = False
            logger.error(f"Unexpected error sending chasing email to {recipient}: {e}")
        
        # Update notification tracking
        update_chasing_email_notification(is_okay, recipient, user_data)
    
    server.quit()


def update_chasing_email_notification(is_success, email, user_data):
    """
    Update UserNotification table after sending chasing email
    
    Args:
        is_success (bool): Whether email was sent successfully
        email (str): Recipient email address
        user_data (dict): User data containing ownership information
    """
    if is_success:
        finished = '1'
        err = '0'
    else:
        finished = '1'
        err = '1'
    
    # Calculate next chasing count
    current_chase_count = user_data.get('total_chasing_count', 0)
    next_chase_count = current_chase_count + 1
    
    # Get FileOwnership IDs for this user to track notifications
    connection_string = config.mssql_conn
    engine = create_engine(connection_string)
    
    # Insert notification records for each ownership with incremented chasing count
    query = text("""
        INSERT INTO UserNotification(OwnershipId, NotificationDate, NotificationType, Finished, IsError, ChasingCount)
        SELECT fop.ID, SYSDATETIME(), 'c', :finished, :err, :chasing_count
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
    
    try:
        with engine.connect() as connection:
            result = connection.execute(query, {
                "finished": finished,
                "err": err,
                "email": email,
                "chasing_count": next_chase_count
            })
            ret = result.rowcount
            connection.commit()
            logger.info(f"Updated {ret} notification records for chasing email #{next_chase_count} to {email}")
            return ret
    except Exception as e:
        logger.error(f"Database error updating chasing email notification: {str(e)}")
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
                
                for user_data in users_to_chase:
                    try:
                        # Create email content
                        html_content, text_content = create_chasing_email_content(user_data)
                        
                        # Send chasing email
                        send_chasing_email(
                            torecipients=[user_data['email']],
                            ccrecipients=[],
                            bccrecipients=[],
                            html=html_content,
                            emailsubject="Reminder: HR Data Remediation Required - Action Needed",
                            user_data=user_data
                        )
                        
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
-- Test Data Creation Script for Chasing Email Testing
-- Creates test notifications with backdated timestamps for given PSIDs

-- STEP 1: Create test data for specific PSIDs
-- Replace these PSIDs with actual ones from your FileOwnership table
DECLARE @TestPSIDs TABLE (PSID varchar(20));
INSERT INTO @TestPSIDs VALUES 
    ('TEST_PSID_001'),
    ('TEST_PSID_002'), 
    ('TEST_PSID_003');

-- STEP 2: Insert initial 'm' type notifications (backdated 8+ days)
-- This simulates users who received HR notifications over a week ago
INSERT INTO UserNotification (OwnershipId, NotificationDate, NotificationType, Finished, IsError, ChasingCount)
SELECT 
    fop.ID as OwnershipId,
    DATEADD(day, -8, GETUTCDATE()) as NotificationDate,  -- 8 days ago
    'm' as NotificationType,
    1 as Finished,
    0 as IsError,
    0 as ChasingCount
FROM FileOwnership fop
JOIN @TestPSIDs tp ON fop.PSID = tp.PSID
JOIN DIRaw didat ON didat.Id = fop.FileID
WHERE didat.Load_For = 'HR'
  AND NOT EXISTS (
      SELECT 1 FROM UserNotification un 
      WHERE un.OwnershipId = fop.ID 
        AND un.NotificationType = 'm'
  );

-- STEP 3: Optionally add some chasing email notifications (for testing escalation)
-- Uncomment to simulate users who already received 1-2 chasing emails
/*
INSERT INTO UserNotification (OwnershipId, NotificationDate, NotificationType, Finished, IsError, ChasingCount)
SELECT 
    fop.ID as OwnershipId,
    DATEADD(day, -3, GETUTCDATE()) as NotificationDate,  -- 3 days ago
    'c' as NotificationType,
    1 as Finished,
    0 as IsError,
    1 as ChasingCount
FROM FileOwnership fop
JOIN @TestPSIDs tp ON fop.PSID = tp.PSID
JOIN DIRaw didat ON didat.Id = fop.FileID
WHERE didat.Load_For = 'HR'
  AND fop.PSID = 'TEST_PSID_002'  -- Only for one test user
  AND NOT EXISTS (
      SELECT 1 FROM UserNotification un 
      WHERE un.OwnershipId = fop.ID 
        AND un.NotificationType = 'c'
  );
*/

-- STEP 4: Verification query - Check what test data was created
SELECT 
    fo.OwnerEmail,
    fo.OwnerName,
    fop.PSID,
    un.NotificationType,
    un.NotificationDate,
    un.ChasingCount,
    un.Finished,
    un.IsError
FROM UserNotification un
JOIN FileOwnership fop ON un.OwnershipId = fop.ID
JOIN FileOwner fo ON fop.PSID = fo.PSID
JOIN @TestPSIDs tp ON fop.PSID = tp.PSID
ORDER BY fo.OwnerEmail, un.NotificationDate;

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