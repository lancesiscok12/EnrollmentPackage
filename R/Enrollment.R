Enrollment<-function(myschoolyear=2016,
                     currentDate=paste(substring(Sys.Date(),6,7),substring(Sys.Date(),9,10),substring(Sys.Date(),1,4),sep='/'),
                     maxDate='12/31/9999',
                     PrimaryOnly='N',
                     EnrolledOn=NULL,
                     includeFRL='N',
                     includeSPED='N',
                     includeELL='N',
                     includeRace='N',
                     includeTitleITargeted='N',
                     includeTitleISchool='N',
                     includeLAP='N'){
  
  library(data.table)
  library(RODBC)
  library(lubridate)
  library(dplyr)
  
  if (PrimaryOnly=='N'){primary="'Y','N'"}
  if (PrimaryOnly=='Y'){primary="'Y'"}
  if (!is.null(EnrolledOn)) {if (is.na(mdy(EnrolledOn))) {return("EnrolledOn must by DD/MM/YYYY Format")}}
  if (is.na(mdy(currentDate))) {return("currentDate must by DD/MM/YYYY Format")}
  
  
  
  myQuery<-function(query){
    cedars <- odbcDriverConnect('driver={SQL Server};server=srv-sql03;database=cedars;trusted_connection=TRUE')
    dat<-sqlQuery(cedars,query,as.is=T) %>%
      filter(RowNumber==1) %>% 
      select(-RowNumber)%>% 
      as.data.table() 
    return(dat)
  }
  
  cedars <- odbcDriverConnect('driver={SQL Server};server=srv-sql03;database=cedars;trusted_connection=TRUE')
  
  DSE<-sqlQuery(cedars,paste("Select  schoolYear,
                             districtOrganizationId,
                             ssid,
                             dateEnrolledinDistrict,
                             case when dateExitedDistrict is null then '",maxDate,"' else dateexiteddistrict end as dateExitedDistrict,
                             gradelevelid
                             From cedars..DistrictStudentEnrollment 
                             Where SchoolYear=",myschoolyear,"
                             AND	recStartDate <= '",currentDate,"'
                             And	isnull(recEndDate,'",maxDate,"') >= '",currentDate,"'",sep=''),as.is=T) %>%
    mutate(dateEnrolledinDistrict=ymd_hms(dateEnrolledinDistrict),
           dateExitedDistrict=ymd_hms(dateExitedDistrict)) %>%
    filter(!(dateExitedDistrict<dateEnrolledinDistrict)) %>%
    as.data.table()
  
  SSE<-sqlQuery(cedars,paste("Select schoolYear,
                             districtOrganizationId,
                             schoolOrganizationId,
                             ssid,
                             dateEnrolledinSchool,
                             case when dateExitedSchool is null then '",maxDate,"' else dateExitedSchool end as dateExitedSchool
                             From cedars..SchoolStudentEnrollment 
                             Where SchoolYear=",myschoolyear,"
                             AND	recStartDate <= '",currentDate,"'
                             And	isnull(recEndDate,'",maxDate,"') >= '",currentDate,"'
                             And isPrimarySchool in (",primary,")",sep=''),as.is=T) %>%
    mutate(dateEnrolledinSchool=ymd_hms(dateEnrolledinSchool),
           dateExitedSchool=ymd_hms(dateExitedSchool)) %>%
    filter(!(dateExitedSchool<dateEnrolledinSchool)) %>%
    as.data.table()
  
  setkey(DSE,schoolYear,districtOrganizationId,ssid,dateEnrolledinDistrict,dateExitedDistrict)
  setkey(SSE,schoolYear,districtOrganizationId,ssid,dateEnrolledinSchool,dateExitedSchool)
  enr<-foverlaps(DSE, SSE,type='start')
  
  myQueriesDistrict<-list()
  myQueriesschool<-list()
  
  
  if (includeRace=='Y'){
    raceQuery<-paste("Select schoolYear,districtOrganizationId,ssid,racetyperollupid,1 as RowNumber
                     from studentracetyperollup
                     Where SchoolYear=",myschoolyear,"
                     AND	recStartDate <= '",currentDate,"'
                     And	isnull(recEndDate,'",maxDate,"') >= '",currentDate,"'",sep='')
    myQueriesDistrict[length(myQueriesDistrict)+1]<-raceQuery}
  
  if (includeSPED=='Y'){
    spedQuery<-paste("Select schoolYear,districtOrganizationId,ssid,lretypeid,
                     ROW_NUMBER() over (Partition by SSID, districtOrganizationId order by startdate desc) as RowNumber
                     from studentspecialed
                     Where SchoolYear=",myschoolyear,"
                     AND	recStartDate <= '",currentDate,"'
                     And	isnull(recEndDate,'",maxDate,"') >= '",currentDate,"'",sep='')
    myQueriesDistrict[length(myQueriesDistrict)+1]<-spedQuery}
  
  
  if (includeFRL=='Y'){
    frlQuery<-paste("Select schoolYear,districtOrganizationId,schoolOrganizationId,ssid,mealstatustypeid,
                    ROW_NUMBER() over (Partition by SSID, districtOrganizationId, schoolOrganizationId order by startdate desc) as RowNumber
                    from studentmealstatus
                    Where SchoolYear=",myschoolyear,"
                    AND	recStartDate <= '",currentDate,"'
                    And	isnull(recEndDate,'",maxDate,"') >= '",currentDate,"'",sep='')
    myQueriesschool[length(myQueriesschool)+1]<-frlQuery}
  
  if (includeELL=='Y'){
    ellQuery<-paste("Select schoolYear,districtOrganizationId,schoolOrganizationId,ssid,'ELL' as ELL,
                    ROW_NUMBER() over (Partition by SSID, districtOrganizationId, schoolOrganizationId order by startdate desc) as RowNumber
                    from studentbilingual
                    Where SchoolYear=",myschoolyear,"
                    AND	recStartDate <= '",currentDate,"'
                    And	isnull(recEndDate,'",maxDate,"') >= '",currentDate,"'",sep='')
    myQueriesschool[length(myQueriesschool)+1]<-ellQuery}
  
  
  if (includeTitleITargeted=='Y'){
    TitleITargetedQuery<-paste("Select distinct schoolYear,districtOrganizationId,schoolOrganizationId,ssid,programid as TitleITargetedID, 
                               ROW_NUMBER() over (Partition by SSID, districtOrganizationId, schoolOrganizationId,programid order by startdate desc) as RowNumber
                               from studentprogram
                               Where SchoolYear=",myschoolyear,"
                               AND	recStartDate <= '",currentDate,"'
                               And	isnull(recEndDate,'",maxDate,"') >= '",currentDate,"'
                               and programid in (8,9,10,12,13)",sep='')
    myQueriesschool[length(myQueriesschool)+1]<-TitleITargetedQuery}
  
  if (includeTitleISchool=='Y'){
    TitleISchoolQuery<-paste("Select distinct schoolYear,districtOrganizationId,schoolOrganizationId,ssid,programid as TitleISchoolID ,
                             ROW_NUMBER() over (Partition by SSID, districtOrganizationId, schoolOrganizationId,programid order by startdate desc) as RowNumber
                             from studentprogram
                             Where SchoolYear=",myschoolyear,"
                             AND	recStartDate <= '",currentDate,"'
                             And	isnull(recEndDate,'",maxDate,"') >= '",currentDate,"'
                             and programid in (21,22,26,27)",sep='')
    myQueriesschool[length(myQueriesschool)+1]<-TitleISchoolQuery}
  
  if (includeLAP=='Y'){
    lapQuery<-paste("Select distinct schoolYear,districtOrganizationId,schoolOrganizationId,ssid,programid as LAPProgramID,
                    ROW_NUMBER() over (Partition by SSID, districtOrganizationId, schoolOrganizationId,programid order by startdate desc) as RowNumber
                    from studentprogram
                    Where SchoolYear=",myschoolyear,"
                    AND	recStartDate <= '",currentDate,"'
                    And	isnull(recEndDate,'",maxDate,"') >= '",currentDate,"'
                    and programid in (4,5,6,7,37,38)",sep='')
    myQueriesschool[length(myQueriesschool)+1]<-lapQuery}
  
  
  if (any(exists('raceQuery'),exists('spedQuery'))){
    District<-Reduce(full_join,lapply(myQueriesDistrict,function(x) myQuery(x))) %>%
      as.data.table()
    enr<-left_join(enr,District)
  }
  
  
  if (any(exists('ellQuery'),exists('frlQuery'),exists('TitleITargetedQuery'),
          exists('Title1SchoolQuery'),exists('lapQuery'))){
    Schools<-Reduce(full_join,lapply(myQueriesschool,function(x) myQuery(x))) %>%
      as.data.table()
    enr<-left_join(enr,Schools)
  }
  
  return(enr)
}
