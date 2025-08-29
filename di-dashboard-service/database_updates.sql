-- Database schema updates for chasing email tracking
-- Adds columns to support chasing email count and manager escalation

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

-- Optional: Add manager email column to FileOwner table for future escalation
-- ALTER TABLE DIDashboard.dbo.FileOwner 
-- ADD ManagerEmail varchar(200) COLLATE Latin1_General_CI_AS NULL;

-- delete user sqls:
DECLARE @PSID varchar(20) = 'PSID_GOES_HERE';

BEGIN TRAN;

-- All ownerships for this user (and the files they point to)
WITH Ownerships AS (
    SELECT fo.ID AS OwnershipId, fo.FileID
    FROM dbo.FileOwnership fo
    WHERE fo.PSID = @PSID
)
-- 1) Delete leaf tables that FK -> FileOwnership
DELETE ua
FROM dbo.UserAction ua
JOIN Ownerships o ON o.OwnershipId = ua.OwnershipId;

DELETE la
FROM dbo.LabelAction la
JOIN Ownerships o ON o.OwnershipId = la.OwnershipId;

DELETE da
FROM dbo.DeleteAction da
JOIN Ownerships o ON o.OwnershipId = da.OwnershipId;

DELETE un
FROM dbo.UserNotification un
JOIN Ownerships o ON o.OwnershipId = un.OwnershipId;

-- 2) If any remaining rows reference this user as PreviousOwner, null them out
UPDATE fo
SET PreviousOwner = NULL
WHERE fo.PreviousOwner = @PSID;

-- 3) Remove the user's ownership rows
DELETE fo
FROM dbo.FileOwnership fo
JOIN Ownerships o ON o.OwnershipId = fo.ID;