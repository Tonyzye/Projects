if exists (select * from dbo.sysobjects where id = object_id(N'[dbo].[usp_Transfer_OWQ]') and OBJECTPROPERTY(id, N'IsProcedure') = 1)
drop procedure [dbo].[usp_Transfer_OWQ]
GO

SET QUOTED_IDENTIFIER OFF 
GO
SET ANSI_NULLS OFF 
GO


CREATE     procedure dbo.usp_Transfer_OWQ
/* Transfer data from OWQ tables (OWQ_Activity and OWQ_Plan)to ViewPoint tables (OWQ_Activity_VP and OWQ_Plan_VP)
** 2006.07.10 Tye   Initial version
** 2006.09.06 Tye   Add post-transfer clean-up process
** 2006.09.07 Tye   Add ClientName to ##TempOWQ_Plan.  Combine Insured1's FirstName, 
**    MiddleName and LastName to ClientName.
** 2006.09.08 Tye   Use Top 1 for inserting OWQ_Plan_VP in case planname is not unique.
**                Update existing OWQ_Activity_VP records from View or Modify buttons
** 2006.09.11 Tye  Check record count before updating Last_Id_Value.  When there is no record, 
**                do not update the count.  Add 2 to @rcount for @act_end_id instead of 20.
** 2006.10.02 Tye    Update Client_Name field formula when Insured1LastName or Insured1FirstName is Null
**                   Update the Cleanup sequence to remove "Deleted" OWQ_Activity_VP before Deleted OWQ_Activity
*/
AS
begin

-- SET NOCOUNT to ON and no longer display the count message.
SET NOCOUNT ON

    declare @act_begin_id binary(8)
    declare @act_end_id binary(8)
    declare @plan_begin_id binary(8)
    declare @plan_end_id binary(8)
    declare @rcount int
    declare @base int

    declare @Rn_Create_User binary(8)
    declare @Rn_Create_Date datetime
    declare @Rn_Edit_User binary(8)
    declare @Rn_Edit_Date datetime

    select @Rn_Create_User = Users_id
    from  Users
    where login_name = 'CRMDSM'

    select @Rn_Create_Date = getdate()

    select @Rn_Edit_User = Users_id
    from  Users
    where login_name = 'CRMDSM'

    select @Rn_Edit_Date = getdate()

-- Put completed OWQ_Activity into temp table
-- select * from ##TempOWQ_Act
--  select * from owq_activity    
    Select OWQ_Activity_Id,
            Username,
            OWQDateTime
    into ##TempOWQ_Act
    from OWQ_Activity
    where OWQStatus like 'Completed'
        and exists (select * from OWQ_Activity_VP vp
                      where vp.OWQ_Activity_VP_Id = OWQ_Activity.OWQ_Activity_Id)

-- Put completed OWQ_Plan into temp table
-- select * from ##TempOWQ_Plan
    select
            OWQ_Activity_Id,
            PlanName,
            AnnualPremium,
            ConceptName,
            DeathBenefit,
            Exchange1035,
            IllustrationType,
            Insured1Age,
            Insured1Sex,
            Insured1Status,
            Insured1TableRating,
            Insured2Age,
            Insured2Sex,
            Insured2Status,
            Insured2TableRating,
            LogDateTime,
            LogPlan,
            MultiLife,
            NumOfLives,
            NumOfOutputFilesEmailed,
            NumOfOutputFilesGenerated,
            NumOfPlanCalcs,
            NumOfSecsPlanInNavigator,
            OutputEmailed,
            OutputTypeSent,
            OutputURL,
            PolicyNumber,
            ProductName,
            TargetPremium,
            Sent,
            Edited,
            Rerun,
            Send_Method,
            PlanIndex,
            case 
                when len(ISNULL(Insured1LastName,'')) = 0 and len(ISNULL(Insured1FirstName,'')) = 0 then 
                            ISNULL(Insured1MiddleName,'')
                when len(ISNULL(Insured1LastName,'')) = 0 then 
                            ISNULL(Insured1FirstName,'') + ' ' + ISNULL(Insured1MiddleName,'')        
                else
                    ISNULL(Insured1LastName,'') + ', ' + ISNULL(Insured1FirstName,'') 
                            + ' ' + ISNULL(Insured1MiddleName,'') 
            end as ClientName
    into ##TempOWQ_Plan
    from OWQ_Plan
    where exists (select * 
                        from OWQ_Activity, OWQ_Activity_VP vp 
                        where OWQ_Activity.OWQ_Activity_Id = OWQ_Plan.OWQ_Activity_Id
                            and OWQ_Activity.OWQStatus like 'Completed'
                            and OWQ_Activity.OWQ_Activity_Id = vp.OWQ_Activity_VP_Id
                   )

-- Update Last_Id_Value for OWQ_Activity_VP table
    select @rcount = count(*)
    from ##TempOWQ_Act

    select @act_begin_id = Last_Id_Value
    from  rsys_last_id
    where Table_Name = 'OWQ_Activity_VP'

if @rcount > 0
begin
    set @act_end_id = CONVERT(binary(4), LEFT(@act_begin_id, 4)) +
        CONVERT(binary(4), (CONVERT(binary(4), RIGHT(@act_begin_id, 4)) + (@rcount+2)))

    update rsys_last_id
    set  last_id_value = @act_end_id
    where table_name = 'OWQ_Activity_VP'
end
-- Update Last_Id_Value for OWQ_Plan_VP table
    select @rcount = count(*)
    from ##TempOWQ_Plan

    select @plan_begin_id = Last_Id_Value
    from  rsys_last_id
    where Table_Name = 'OWQ_Plan_VP'

if @rcount > 0
begin
    set @plan_end_id = CONVERT(binary(4), LEFT(@plan_begin_id, 4)) +
        CONVERT(binary(4), (CONVERT(binary(4), RIGHT(@plan_begin_id, 4)) + (@rcount+2)))

    update rsys_last_id
    set  last_id_value = @plan_end_id
    where table_name = 'OWQ_Plan_VP'
end

-- Transfer OWQ Recrods

declare @act_id binary(8)
declare @plan_id binary(8)
declare @OWQact_id binary(8)
declare @username varchar(30)
declare @VPStatus varchar(20)
declare @OWQdatetime datetime
declare @planname varchar(80)

set @act_id = @act_begin_id
set @plan_id = @plan_begin_id

declare activity_cursor cursor for 
    select OWQ_Activity_Id, Username, OWQDateTime from ##TempOWQ_Act

open activity_cursor
fetch next from activity_cursor into @OWQact_id, @username, @OWQdatetime
while @@fetch_status = 0
begin
    select @VPStatus = OWQStatus
        from OWQ_Activity_VP
        where OWQ_Activity_VP_Id = @OWQact_id

    -- When OWQ_Activity_VP status is New, update existing record.
    if upper(@VPStatus) = 'NEW'
    begin
        -- Update From_OWQ flag and other fields
        update OWQ_Activity_VP
        set Rn_Edit_Date = @Rn_Edit_Date,
            Rn_Edit_User = @Rn_Edit_User,
            OWQStatus = 'Completed',
            OWQDateTime = @OWQDatetime,
            Activity_UTCDate = @OWQDatetime,
            Activity_UTCTime = @OWQDatetime,
            OWQ_username = ltrim(rtrim(@username)),
            From_ViewPoint = 0,
            From_OWQ = 1
        where OWQ_Activity_VP_Id = @OWQact_id

        -- Copy CSX data
        update vp
        set NavigatorInputData = owq.NavigatorInputData,
            HasCSX =1
        from OWQ_Activity_VP vp 
           inner join OWQ_Activity owq on vp.OWQ_Activity_VP_Id = owq.OWQ_Activity_Id
        where VP.OWQ_Activity_VP_Id = @OWQact_id


        -- Insert OWQ_Plan_VP
        declare plan_cursor cursor for 
            select planname 
            --from OWQ_Plan
            from ##TempOWQ_Plan
            where OWQ_Activity_Id = @OWQact_id
        
        open plan_cursor
        fetch next from plan_cursor into @planname
        while @@fetch_status = 0
        begin
            -- Get next OWQ_Plan_VP_Id
            set @plan_id = cast(@plan_id + 1 as binary(8))
            
            insert OWQ_Plan_VP
            (
            OWQ_Plan_VP_Id,
            Rn_Descriptor,
            Rn_Create_Date,
            Rn_Create_User,
            Rn_Edit_Date,
            Rn_Edit_User,
            PlanName,
            OWQ_Activity_VP_Id,
            ProductName,
            ConceptName,
            IllustrationType,
            TargetPremium,
            AnnualPremium,
            DeathBenefit,
            Exchange1035,
            NumOfLives,
            Insured1Age,
            Insured1Sex,
            Insured1Status,
            Insured1TableRating,
            Insured2Age,
            Insured2Sex,
            Insured2Status,
            Insured2TableRating,
            PolicyNumber,
            LogPlan,
            OutputEmailed,
            LogDateTime,
            OutputTypeSent,
            OutputURL,
            MultiLife,
            NumOfSecsPlanInNavigator,
            NumOfPlanCalcs,
            NumOfOutputFilesGenerated,
            NumOfOutputFilesEmailed,
            --Transferred,
            Sent,
            Edited,
            Send_Method,
            Rerun,
            PlanIndex,
            ClientName
            )
            select top 1
            @plan_id,
            PlanName,
            @Rn_Create_Date,
            @Rn_Create_User,
            @Rn_Edit_Date,
            @Rn_Edit_User,
            PlanName,
            @OWQact_id,  -- existing OWQ_Activity_VP_Id
            ProductName,
            ConceptName,
            IllustrationType,
            TargetPremium,
            AnnualPremium,
            DeathBenefit,
            Exchange1035,
            NumOfLives,
            Insured1Age,
            Insured1Sex,
            Insured1Status,
            Insured1TableRating,
            Insured2Age,
            Insured2Sex,
            Insured2Status,
            Insured2TableRating,
            PolicyNumber,
            LogPlan,
            OutputEmailed,
            LogDateTime,
            OutputTypeSent,
            OutputURL,
            MultiLife,
            NumOfSecsPlanInNavigator,
            NumOfPlanCalcs,
            NumOfOutputFilesGenerated,
            NumOfOutputFilesEmailed,
            --Transferred,
            Sent,
            Edited,
            Send_Method,
            Rerun,
            PlanIndex,
            ClientName
            from ##TempOWQ_Plan
            where OWQ_Activity_Id = @OWQact_id
                and planname = @planname
        
        fetch next from plan_cursor into @planname
        end
        close plan_cursor
        deallocate plan_cursor

    end
    else
    -- When OWQ_Activity_VP status is not New, create new record.
    begin

        -- Update From_OWQ flag and other fields
        update OWQ_Activity_VP
        set Rn_Edit_Date = @Rn_Edit_Date,
            Rn_Edit_User = @Rn_Edit_User,
            OWQStatus = 'Completed',
            From_ViewPoint = 0,
            From_OWQ = 0
        where OWQ_Activity_VP_Id = @OWQact_id

        -- Get next OWQ_Activity_VP_Id
        set @act_id = cast(@act_id + 1 as binary(8))

        -- Create a new OWQ_Activity_Vp record
        insert OWQ_Activity_VP (
            OWQ_Activity_VP_Id,
            Rn_Descriptor,
            Rn_Create_Date,
            Rn_Create_User,
            Rn_Edit_Date,
            Rn_Edit_User,
            OWQStatus,
            OWQDateTime,
            Activity_UTCDate,
            Activity_UTCTime,
            Contact_Id,
            StateLicenseNumbers,
            BrokerDealer_Id,
            Title,
            First_Name,
            Middle_Name,
            Last_Name,
            Suffix,
            Credentials,
            Location_Address_1,
            Location_City,
            Location_State,
            Location_Zip,
            Email,
            BrokerDealerName,
            Opportunity_Id,
            Opportunity_Name,
            PlanNameSelected,
            Create_Username,
            OWQ_username,
            From_ViewPoint,
            From_OWQ,
            HasCSX,
            NavigatorInputData )
        select 
            @act_id,
            vp.Rn_Descriptor,
            @Rn_Create_Date,
            vp.Rn_Create_User,
            @Rn_Edit_Date,
            @Rn_Edit_User,
            'Completed',
            @OWQDateTime,
            @OWQDateTime,
            @OWQDateTime,
            vp.Contact_Id,
            vp.StateLicenseNumbers,
            vp.BrokerDealer_Id,
            vp.Title,
            vp.First_Name,
            vp.Middle_Name,
            vp.Last_Name,
            vp.Suffix,
            vp.Credentials,
            vp.Location_Address_1,
            vp.Location_City,
            vp.Location_State,
            vp.Location_Zip,
            vp.Email,
            vp.BrokerDealerName,
            vp.Opportunity_Id,
            vp.Opportunity_Name,
            vp.PlanNameSelected,
            vp.create_username,
            @Username,
            0,
            1,
            1,
            owq.NavigatorInputData   --- CSX Data
        from OWQ_Activity_VP vp, OWQ_Activity owq
        where vp.OWQ_Activity_VP_Id = @OWQact_id
            and vp.OWQ_Activity_VP_Id = owq.OWQ_Activity_Id

        -- Copy CSX data
        -- CSX data field is also included in the above statement

        -- Insert OWQ_Plan_VP
        declare plan_cursor cursor for 
            select planname 
            --from OWQ_Plan
            from ##TempOWQ_Plan
            where OWQ_Activity_Id = @OWQact_id
        
        open plan_cursor
        fetch next from plan_cursor into @planname
        while @@fetch_status = 0
        begin
            -- Get next OWQ_Plan_VP_Id
            set @plan_id = cast(@plan_id + 1 as binary(8))
            
            insert OWQ_Plan_VP
            (
            OWQ_Plan_VP_Id,
            Rn_Descriptor,
            Rn_Create_Date,
            Rn_Create_User,
            Rn_Edit_Date,
            Rn_Edit_User,
            PlanName,
            OWQ_Activity_VP_Id,
            ProductName,
            ConceptName,
            IllustrationType,
            TargetPremium,
            AnnualPremium,
            DeathBenefit,
            Exchange1035,
            NumOfLives,
            Insured1Age,
            Insured1Sex,
            Insured1Status,
            Insured1TableRating,
            Insured2Age,
            Insured2Sex,
            Insured2Status,
            Insured2TableRating,
            PolicyNumber,
            LogPlan,
            OutputEmailed,
            LogDateTime,
            OutputTypeSent,
            OutputURL,
            MultiLife,
            NumOfSecsPlanInNavigator,
            NumOfPlanCalcs,
            NumOfOutputFilesGenerated,
            NumOfOutputFilesEmailed,
            --Transferred,
            Sent,
            Edited,
            Send_Method,
            Rerun,
            PlanIndex,
            ClientName
            )
            select top 1
            @plan_id,
            PlanName,
            @Rn_Create_Date,
            @Rn_Create_User,
            @Rn_Edit_Date,
            @Rn_Edit_User,
            PlanName,
            @act_id,  -- new OWQ_Activity_VP_Id
            ProductName,
            ConceptName,
            IllustrationType,
            TargetPremium,
            AnnualPremium,
            DeathBenefit,
            Exchange1035,
            NumOfLives,
            Insured1Age,
            Insured1Sex,
            Insured1Status,
            Insured1TableRating,
            Insured2Age,
            Insured2Sex,
            Insured2Status,
            Insured2TableRating,
            PolicyNumber,
            LogPlan,
            OutputEmailed,
            LogDateTime,
            OutputTypeSent,
            OutputURL,
            MultiLife,
            NumOfSecsPlanInNavigator,
            NumOfPlanCalcs,
            NumOfOutputFilesGenerated,
            NumOfOutputFilesEmailed,
            --Transferred,
            Sent,
            Edited,
            Send_Method,
            Rerun,
            PlanIndex,
            ClientName
            from ##TempOWQ_Plan
            where OWQ_Activity_Id = @OWQact_id
                and planname = @planname
        
        fetch next from plan_cursor into @planname
        end
        close plan_cursor
        deallocate plan_cursor

    end

fetch next from activity_cursor into @OWQact_id, @username, @OWQdatetime
end
close activity_cursor
deallocate activity_cursor

--*** Clean up OWQ tables ***
-- Remove OWQ_Plan before removing OWQ_Activity
-- Remove "Completed" OWQ Plan
delete OWQ_Plan
-- select * from OWQ_Plan
where exists 
    (Select * from ##TempOWQ_Plan t
        where t.OWQ_Activity_Id = OWQ_Plan.OWQ_Activity_Id
            and t.PlanName = OWQ_Plan.PlanName 
    )

-- Remove "Completed" OWQ Activity
Delete OWQ_Activity
--select * from OWQ_Activity
where exists 
    (Select * from ##TempOWQ_Act t
        where t.OWQ_Activity_Id = OWQ_Activity.OWQ_Activity_Id
            --and OWQ_Activity.OWQStatus like 'Completed'
    )

-- Remove OWQ Plan for "Deleted" activity
delete OWQ_Plan
-- select * from OWQ_Plan
where exists 
    (Select * from OWQ_Activity owq
        where owq.OWQ_Activity_Id = OWQ_Plan.OWQ_Activity_Id
            and owq.OWQStatus like 'Deleted' 
    )


-- *** Clean up ViewPoint tables ***
-- Remove "Deleted" OWQ Activity_VP that has no CSX associated
Delete vp
from OWQ_Activity_VP vp
--select * from OWQ_Activity_VP vp
where exists 
    (Select * from OWQ_Activity owq
        where owq.OWQ_Activity_Id = vp.OWQ_Activity_VP_Id
            and owq.OWQStatus like 'Deleted'
    )
and (vp.HasCSX is null or vp.HasCSX = 0)

-- Remove "Deleted" OWQ Activity after removing those in ViewPoint

Delete owq
from OWQ_Activity owq
--select * from OWQ_Activity owq
where owq.OWQStatus like 'Deleted'

-- Remove "Completed" OWQ Activity_VP that has no CSX associated
Delete vp
from OWQ_Activity_VP vp
--select * from OWQ_Activity_VP vp
where not exists 
    (Select * from OWQ_Activity owq
        where owq.OWQ_Activity_Id = vp.OWQ_Activity_VP_Id
            and owq.OWQStatus like 'Completed'
    )
and (vp.HasCSX is null or vp.HasCSX = 0)
and vp.OWQStatus like 'Completed'


drop table ##TempOWQ_Act
drop table ##TempOWQ_Plan



END


GO
SET QUOTED_IDENTIFIER OFF 
GO
SET ANSI_NULLS ON 
GO
