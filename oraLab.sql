SET SERVEROUTPUT ON

CREATE OR REPLACE PACKAGE BODY PKG_2017Main AS
--
-- Package: PKG_2017Main
-- Author:  Kisung Tae
-- Date:    10-May-2017

-- Description
    -- This package is to forecast an electricity consumption for 
    -- each TNI, LR, FRMP combination, daily at each half hour interval.
    -- It also contains the functions and procedures to generate electricity report
    -- files in xml format and send them as attachments. 

--
-- Constant variables
v_holiday_mark      VARCHAR2(30); 
v_forecast_mark     VARCHAR2(30);
v_sunday_mark       VARCHAR2(30);
v_day_format        VARCHAR2(20);
v_date_format       VARCHAR2(30);
v_report_prefix     VARCHAR(40);
v_report_counter    NUMBER := 3;
v_forecast_counter  NUMBER := 7;


TYPE consumptionReport is RECORD (
  title VARCHAR2(100),
  material CLOB);
  
TYPE consumptionReportArray is TABLE of consumptionReport
  INDEX BY BINARY_INTEGER; 



FUNCTION f_get_codes_value(p_kind VARCHAR2, p_code VARCHAR2) RETURN VARCHAR2 IS
--
-- Notes by Kisung Tae
-- This function will return corresponding values to the combination of
-- kind and code attributes from "PARAMETER" table which holds various
-- constant values, referencing pk_EA_UTILITIES.pkb.

v_value     PARAMETER.VALUE%TYPE; 
v_func_name VARCHAR(50) := 'f_get_codes_value';
-- 
BEGIN
   SELECT VALUE INTO v_value
   FROM parameter
   WHERE kind = p_kind
   AND   code = p_code
   AND NVL(active, 'N') = 'Y';
   --
   RETURN v_value;
   --
EXCEPTION
  WHEN NO_DATA_FOUND THEN
      COMMON.log('Exception in ' || v_func_name || ' ' ||'NO DATA FOUND ERROR');
  WHEN TOO_MANY_ROWS THEN
      COMMON.log('Exception in ' || v_func_name || ' ' ||'TOO MANY ROWS ERROR');
  WHEN OTHERS THEN 
      COMMON.log('Exception in ' || v_func_name || ' ' ||SQLERRM);
  
  RAISE;   
END;  


PROCEDURE p_populate_constant_variables IS
--
-- Notes by Kisung Tae
-- This procedure is implemented to populate constant variables in one place
-- so that it can be easy to modify codes for populating constant variables

BEGIN
  v_holiday_mark := f_get_codes_value('MARK','HOLIDAY_MARK');
  v_forecast_mark := f_get_codes_value('MARK','FORECAST_MARK');
  v_sunday_mark := f_get_codes_value('MARK','SUNDAY_MARK');
  v_day_format := f_get_codes_value('FORMAT','DAY_FORMAT');
  v_date_format := f_get_codes_value('FORMAT','DATE_FORMAT');
  v_report_prefix := f_get_codes_value('PREFIX','REPORT_PREFIX');
END;



FUNCTION f_get_dayofweek_to_forecast(p_date_to_forecast DATE) RETURN VARCHAR2 IS
--
-- Notes by Kisung Tae
-- This function will return corresponding day of a week to the parameter's date, p_date_to_forecast.
-- If the parameter's date matches one of the records in "Holiday" table, meaning that the date
-- is holiday, then it will return v_holiday_mark, which represents "HOLIDAY". It will be used
-- denote between day of week and public holiday on the date to forecast. 

v_holiday_row_count NUMBER;

BEGIN
  SELECT COUNT(*) INTO v_holiday_row_count
  FROM DBP_HOLIDAY hd
  WHERE TRUNC(hd.HOLIDAY_DATE) = TRUNC(p_date_to_forecast);
  
  IF v_holiday_row_count > 0 THEN
    RETURN v_holiday_mark;
  END IF;
  
  RETURN trim(to_char(p_date_to_forecast, v_day_format));
END;


PROCEDURE p_forecast_consumption_on(p_date_to_forecast DATE) AS
--
-- Notes by Kisung Tae
-- Primise: The program categorizes the consumption records into eight types of day of a week,
-- including the normal day of week (Monday to Sunday) plus 'HOLIDAY', which indicates
-- the dates that match with the HOLIDAY table

-- This procedure firstly gets the day of week from the date passed by using 
-- f_get_dayofweek_to_forecast function.

-- The query for the cursor has inner and outer queries.
-- INNER QUERY joins 'v_nem_rm16' and 'DBP_HOLIDAY' tables and then mark records
-- 'HOLIDAY' if the dates of the records from two tables match, otherwise mark them
-- as corresponding day of week like Monday, Tuesday and so on based on the records'
-- DAY attribute's values.

-- OUTER QUERY gets only the records matching the day of week paased through the parameter
-- then groups them by TNI, FRMP, LR, and HH to get the average volume
-- It works in this way so that it does not have to call a function 
-- which is slower and more memory consuming than a query
-- Every neccesary process to get FORECAST consumption data is done by a query

TYPE r_cursor_rec IS RECORD 
        (TNI VARCHAR2(8),
         FRMP VARCHAR2(20),
         LR VARCHAR2(30),
         HH NUMBER,
         VOLUME NUMBER);

CURSOR c_elec_day_consum(p_day_of_week VARCHAR) IS
      SELECT TNI, FRMP, LR, HH, AVG(Coalesce(VOLUME, 0)) AS VOLUME
      FROM (SELECT v.TNI, v.FRMP, v.LR, v.DAY, v.HH, v.VOLUME,
            CASE WHEN hd.HOLIDAY_DATE is not null then v_holiday_mark
            ELSE trim(to_char(v.DAY, v_day_format)) END AS WEEKDAY
            FROM v_nem_rm16 v
            LEFT JOIN DBP_HOLIDAY hd 
            ON v.DAY = hd.HOLIDAY_DATE
            WHERE v.STATEMENT_TYPE != v_forecast_mark)
      WHERE WEEKDAY = p_day_of_week
      GROUP BY TNI, FRMP, LR, HH;

      
v_day_to_forecast VARCHAR2(100) := f_get_dayofweek_to_forecast(p_date_to_forecast);
v_proc_name VARCHAR(50) := 'p_forecast_consumption_on';
r_elec_consum_row LOCAL_RM16%ROWTYPE;
r_temp_cursor_rec r_cursor_rec;

BEGIN

  -- If the date to forecast is holiday, then check if there is data in the cursor
  -- ,opening and fetching just one row and checking if the fectch succeeds.
  -- If it fails, the day of week will chnage to sunday_mark, which represents "Sunday".
  -- Then, the final cursor to forecast with would be based on Sunday's data. 
  IF v_day_to_forecast = v_holiday_mark THEN
    OPEN c_elec_day_consum(v_day_to_forecast);
    FETCH c_elec_day_consum INTO r_temp_cursor_rec;
    
    IF c_elec_day_consum%NOTFOUND THEN
      v_day_to_forecast := v_sunday_mark;
    END IF;
    CLOSE c_elec_day_consum;
  END IF;  
  
  FOR r_elec_day_consum IN c_elec_day_consum(v_day_to_forecast) LOOP    
      INSERT INTO LOCAL_RM16(TNI, FRMP, LR, STATEMENT_TYPE, DAY, HH, VOLUME) 
              VALUES(r_elec_day_consum.TNI, r_elec_day_consum.TNI, r_elec_day_consum.LR,
              v_forecast_mark, p_date_to_forecast, r_elec_day_consum.HH, r_elec_day_consum.VOLUME);
  END LOOP;
  
EXCEPTION
  WHEN NO_DATA_FOUND THEN
    COMMON.log('Exception in ' || v_proc_name || ' ' ||'NO DATA FOUND ERROR');
  WHEN TOO_MANY_ROWS THEN
    COMMON.log('Exception in ' || v_proc_name || ' ' ||'TOO MANY ROWS ERROR');
  WHEN OTHERS THEN
    COMMON.log('Exception in ' || v_proc_name || ' ' ||SQLERRM);
  
  IF c_elec_day_consum%ISOPEN THEN 
    CLOSE c_elec_day_consum;
  END IF;
  
  RAISE;
    
END;




FUNCTION f_get_consum_report_on(p_report_date DATE) RETURN CLOB AS
--
-- Notes by Kisung Tae
-- This function references the given function by Laurie Benkovich
-- This function gets the FORECAST consumption data on the date passed and
-- make a clob of report based on the data.

Ctx               DBMS_XMLGEN.ctxHandle;
xml               CLOB := NULL;
temp_xml          CLOB := NULL;
v_query_date      VARCHAR2(30) := to_char(p_report_date, v_date_format);
QUERY             VARCHAR2(2000) := 'SELECT tni, sum(volume) tni_total 
                                    FROM LOCAL_RM16
                                    WHERE to_char(DAY, ''' || v_date_format || ''') = '''||v_query_date||''' GROUP BY tni';
v_proc_name VARCHAR(100) := 'f_get_consum_report_on';

BEGIN
   dbms_output.put_line(QUERY);
   Ctx := DBMS_XMLGEN.newContext(QUERY);
   DBMS_XMLGen.setRowsetTag( Ctx, 'Electricity Consumption' );
   DBMS_XMLGen.setRowTag( Ctx, 'TNI' );
   temp_xml := DBMS_XMLGEN.getXML(Ctx);
--
        IF temp_xml IS NOT NULL THEN
            IF xml IS NOT NULL THEN
                DBMS_LOB.APPEND( xml, temp_xml );
            ELSE
                xml := temp_xml;
            END IF;
        END IF;
--
        DBMS_XMLGEN.closeContext( Ctx );
        dbms_output.put_line(substr(xml, 1, 1950));
        RETURN xml;
        
EXCEPTION
  WHEN OTHERS THEN
    COMMON.log('Exception in ' || v_proc_name || ' ' ||SQLERRM);
  
  RAISE;
END;



PROCEDURE p_write_to_file_from(p_data CLOB, p_file_name VARCHAR2) AS
--
-- Notes by Kisung Tae
-- This procedure will produce a file from CLOB object.
-- Note that UTL_FILE.PUT_LINE will put carrige return at the end on a line
-- This procedure references the code on 
-- <http://www.astral-consultancy.co.uk/cgi-bin/hunbug/doco.cgi?11070>

v_file              utl_file.file_type;
v_buff              VARCHAR(32767);
v_amount            BINARY_INTEGER := 3276;
v_cr                PLS_INTEGER;
v_data_length       PLS_INTEGER;
v_current_position  PLS_INTEGER := 1;
v_file_name         VARCHAR2(100):= p_file_name;
v_proc_name         VARCHAR2(50):= 'p_write_to_file_from';

BEGIN
  v_file := utl_file.fopen ('UTL_FILE_DIRECTORY', v_file_name, 'A');
  v_data_length := DBMS_LOB.GETLENGTH(p_data);
  
  WHILE v_current_position < v_data_length LOOP
    v_buff := DBMS_LOB.SUBSTR(p_data, v_amount, v_current_position);
    EXIT WHEN v_buff IS NULL;
    v_cr := INSTR(v_buff, CHR(10), -1);
    IF v_cr != 0 THEN
      v_buff := SUBSTR(v_buff, 1, v_cr -1);
    END IF;
    UTL_FILE.PUT_LINE(v_file, v_buff, TRUE);
    v_current_position := v_current_position + LEAST(LENGTH(v_buff)+1, v_amount);
  END LOOP;
  
  UTL_FILE.FCLOSE(v_file);
  
EXCEPTION
  WHEN OTHERS THEN
    COMMON.log('Exception in ' || v_proc_name || ' ' ||SQLERRM);
    IF UTL_FILE.IS_OPEN(v_file) THEN 
      UTL_FILE.FCLOSE(v_file);
    END IF; 
  
  RAISE;

END;


PROCEDURE p_send_email_with_attachment(p_consumption_report_array consumptionReportArray) AS
--
-- Notes by Kisung Tae
-- This procedure will send an email with consumption reports as attachments. 
-- The attachments come into this procedure in the from of an array of pairs. 
-- The pairs are report's title that holds the string of the report's title
-- and report's material that holds a clob of report generated.
-- Once the array comes into this procedure we will need to loop through the values 
-- and then add a report material for each entry.
-- The definition for the array has been defined at global scope of this  package body

p_subject       VARCHAR2(50) := f_get_codes_value('EMAIL_SUBJECT','SUBJECT');
p_recipient     VARCHAR2(50) := f_get_codes_value('EMAIL_RECIPIENT','ASSGN_RECIPIENT');
p_sender        VARCHAR2(50) := f_get_codes_value('EMAIL_SENDER','SENDER');
v_boundary_text VARCHAR2(25) := f_get_codes_value('EMAIL_BOUNDARY','BOUNDARY_TEXT');
v_mailhost      VARCHAR2(50) := f_get_codes_value('EMAIL_MAILHOST','MAILHOST');
v_email_footer  VARCHAR(250) := f_get_codes_value('EMAIL_FOOTER','FOOTER');
v_proc_name     VARCHAR(50)  := 'p_send_email_with_attachment';

mail_conn       UTL_SMTP.connection;
con_nl          VARCHAR2(2) := CHR(13)||CHR(10);

BEGIN

  mail_conn := UTL_SMTP.open_connection (v_mailhost, 25);
  UTL_SMTP.helo (mail_conn, v_mailhost);
  UTL_SMTP.mail (mail_conn, p_sender);
  
  UTL_SMTP.rcpt (mail_conn, p_recipient);
  UTL_SMTP.open_data (mail_conn);
  UTL_SMTP.WRITE_DATA(mail_conn,'From' || ':' || p_sender|| con_nl);
  UTL_SMTP.WRITE_DATA(mail_conn,'To'|| ':'|| p_recipient|| con_nl);
  UTL_SMTP.WRITE_DATA(mail_conn,'Subject'|| ':'|| p_subject||con_nl);
  UTL_SMTP.WRITE_DATA(mail_conn,'Mime-Version: 1.0'||con_nl);
  UTL_SMTP.WRITE_DATA(mail_conn,'Content-Type: multipart/mixed; boundary="'||v_boundary_text||'"'||con_nl);
  UTL_SMTP.WRITE_DATA(mail_conn,'--'||v_boundary_text||con_nl);
  UTL_SMTP.WRITE_DATA(mail_conn,'Content-type: text/plain; charset=us-ascii'||con_nl);
  UTL_SMTP.WRITE_DATA(mail_conn,con_nl||'Sent From the OMS Database by the PL/SQL application '||con_nl);
  UTL_SMTP.WRITE_DATA(mail_conn,'The report data is in the attached file'||con_nl||con_nl);
  UTL_SMTP.WRITE_DATA(mail_conn, 'Regards'||con_nl||'The OMS Database'||con_nl||con_nl);
  UTL_SMTP.write_data (mail_conn, con_nl || v_email_footer||con_nl||con_nl);
           
  FOR counter IN 1..p_consumption_report_array.COUNT LOOP
    UTL_SMTP.WRITE_DATA(mail_conn,con_nl||'--'||v_boundary_text||con_nl);
    UTL_SMTP.WRITE_DATA(mail_conn,'Content-Type: application/octet-stream; name="'||p_consumption_report_array(counter).title||'"'||con_nl);
    UTL_SMTP.WRITE_DATA(mail_conn,'Content-Transfer-Encoding: 7bit'||con_nl||con_nl);    --7bit
    UTL_SMTP.WRITE_DATA(mail_conn, p_consumption_report_array(counter).material);
  END LOOP;
           
  UTL_SMTP.WRITE_DATA(mail_conn,con_nl||'--'||v_boundary_text||'--'||con_nl);
  UTL_SMTP.CLOSE_DATA(mail_conn);
  UTL_SMTP.QUIT(mail_conn);

EXCEPTION
   WHEN OTHERS THEN
      COMMON.log('Exception in ' || v_proc_name || ' ' ||SQLERRM);
      UTL_SMTP.close_data (mail_conn);
  
  RAISE;
END;



PROCEDURE p_publish_report AS
--
-- Notes by Kisung Tae
-- This procedure calls the functions and procedures to generate xml files of reports 
-- and send an email witht them as attachments.
-- The v_report_counter means the number of reports that are to be produced for. For example, 
-- If the v_report_counter is set to "3", It will produce reports for three days into the future from today.
-- Threfore, it will generate three xml files of reports and attachments in an email.
-- It is designed in this way so that it can easily deal with the modification in
-- the number of dates for which reports are required to be generated

v_consumption_report consumptionReport;
v_consumption_report_array consumptionReportArray;
v_report_date DATE := SYSDATE;
v_report_file_name VARCHAR2(100);
v_report_clob CLOB;
v_proc_name VARCHAR2(50) := 'p_publish_report';
  
BEGIN  

  -- Make sure the number of report to be produced is less than or equal
  -- to the number of days for which forecasting data is generated
  IF v_report_counter > v_forecast_counter THEN
    COMMON.log('Exception in ' || v_proc_name 
    || ' report counter is modified to be equal to forecasting counter');
    v_report_counter := v_forecast_counter;
  END IF;
  
  FOR counter IN 1..v_report_counter LOOP
      v_report_date := v_report_date + 1;
      v_report_file_name := v_report_prefix || to_char(v_report_date, v_date_format) || '.xml';
      v_report_clob := f_get_consum_report_on(v_report_date);
      p_write_to_file_from(v_report_clob, v_report_file_name);

      v_consumption_report.title := v_report_file_name;
      v_consumption_report.material := v_report_clob;
      v_consumption_report_array(counter) := v_consumption_report;
  END LOOP;
  
  p_send_email_with_attachment(v_consumption_report_array);
  
END;



PROCEDURE RM16_forecast AS
--
-- Notes by Kisung Tae
-- This procedure is starting point of this program.

v_date_to_forecast DATE := sysdate;

BEGIN

  p_populate_constant_variables();

  FOR counter IN 1..v_forecast_counter LOOP
    v_date_to_forecast := v_date_to_forecast + 1;
    p_forecast_consumption_on(v_date_to_forecast); 
  END LOOP;

  p_publish_report();

  COMMIT;

EXCEPTION
    WHEN OTHERS THEN
      COMMON.log('Program ends with error. Check message logs to see specific error');
      ROLLBACK;
      
END;

END PKG_2017Main;



