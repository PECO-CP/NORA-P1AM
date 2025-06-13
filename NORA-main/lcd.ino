/* LCD Functions **************************************************************/

/**
 * @brief STANDBY screen
 * 
 * Displays current time and next sample time
 * 
 *  _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _
 * |C U R R : 0 0 - 0 0 - 0 0 _ 0 0 : 0 0 _|
 * |N E X T : 0 0 - 0 0 - 0 0 _ 0 0 : 0 0 _|
 * |* S E T T I N G S _ _ _ _ S T A N D B Y|
 * |_ R U N _ S A M P L E _ _ _ _ _ M O D E|
 */
void standbyLCD() {
  lcd.setCursor(0,0);
  lcd.print("CURR: ");
  lcd.print(getCurrentDateTime());
  lcd.setCursor(0, 1);
  lcd.print("NEXT: ");
  lcd.print(getNextSampleTime());
  lcd.setCursor(1, 2);
  lcd.print("SETTINGS");
  lcd.setCursor(1, 3);
  lcd.print("RUN SAMPLE");
  lcd.setCursor(13, 2);
  lcd.print("STANDBY");
  lcd.setCursor(16, 3);
  lcd.print("MODE");

  lcd.setCursor(0, cursor_y);
  lcd.print("*");
}

/**
 * @brief "Are you sure?" screen
 * 
 *  _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _
 * |_ _ _ _ R U N _ S A M P L E _ _ _ _ _ _|
 * |_ _ _ A R E _ Y O U _ S U R E ? _ _ _ _|
 * |_ R U N _ S A M P L E _ _ _ _ _ _ _ _ _|
 * |* E X I T _ _ _ _ _ _ _ _ _ _ _ _ _ _ _|
 * 
 * @param ensure the state to enter if confirmed
 */
void ensureLCD(String ensure) {
  lcd.setCursor(4, 0);
  lcd.print(ensure);
  lcd.setCursor(3, 1);
  lcd.print("ARE YOU SURE?");
  lcd.setCursor(1, 2);
  lcd.print(ensure);
  lcd.setCursor(1, 3);
  lcd.print("EXIT");

  lcd.setCursor(0, cursor_y);
  lcd.print("*");
}

/**
 * @brief SETTINGS screen
 * 
 * Each page has a maximum of three setting options
 * 
 * @param page the current settings page to be displayed
 */
void settingsLCD(uint8_t page) {

  switch (page) {
    case 1:
      /*  _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _
       * |* S E T _ I N T E R V A L _ _ _ _ _ _ _|
       * |_ S E T _ S T A R T _ T I M E _ _ _ _ _|
       * |_ S E T _ C L O C K _ _ _ _ _ _ _ _ _ _|
       * |< E X I T _ _ _ _ _ _ _ _ _ _ M O R E >|
       */

      lcd.setCursor(1, 0);
      lcd.print("SET INTERVAL");
      lcd.setCursor(1, 1);
      lcd.print("SET START TIME");
      lcd.setCursor(1, 2);
      lcd.print("SET CLOCK");

      lcd.setCursor(0, 3);
      lcd.print("<EXIT"); // first page has "exit" option to return to standby
      lcd.setCursor(15, 3);
      lcd.print("MORE>");
      break;
    
    case 2:
      /*  _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _
       * |* S O A K _ T I M E _ _ _ _ _ _ _ _ _ _|
       * |_ D R Y _ T I M E _ _ _ _ _ _ _ _ _ _ _|
       * |_ F I L T E R _ S T A T U S _ _ _ _ _ _|
       * |< B A C K _ _ _ _ _ _ _ _ _ _ M O R E >|
       */

      lcd.setCursor(1, 0);
      lcd.print("SOAK TIME");
      lcd.setCursor(1, 1);
      lcd.print("DRY TIME");
      lcd.setCursor(1, 2);
      lcd.print("FILTER STATUS");

      lcd.setCursor(0, 3);
      lcd.print("<BACK"); // final page has no "more" option
      break;
  }

  lcd.setCursor(0, cursor_y);
  lcd.print("*");

}

/**
 * @brief RELEASE screen
 * 
 * Displays the current position of motor releasing sample
 * 
 *  _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _
 * |_ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _|
 * |R E L E A S I N G _ D E V I C E . . . _|
 * |C U R R _ P O S I T I O N : 0 . 0 0 m _|
 * |_ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _|
 * 
 * @param position String of current position of sample device in meters
 */
void releaseLCD(String position, float distance_cm) {
  float tide = distance_cm / 100;
  lcd.setCursor(0,1);
  lcd.print("RELEASING DEVICE...");
  lcd.setCursor(0,2);
  lcd.print("CURR POSITION:");
  lcd.setCursor(15,2);
  lcd.print(position);
  lcd.setCursor(0, 3);
  lcd.print("DROP: ");
  lcd.print(String(tide, 2));
  lcd.print("m");
}

/**
 * @brief SOAK screen
 * 
 * Displays the time remaining in soak procedure
 * 
 *  _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _
 * |_ _ _ _ _ S O A K I N G . . . _ _ _ _ _|
 * |_ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _|
 * |_ _ _ 0 0 _ M I N _ 0 0 _ S E C _ _ _ _|
 * |_ _ _ _ _ R E M A I N I N G _ _ _ _ _ _|
 * 
 * @param min_time String of minutes remaining in soak time
 * @param sec_time String of seconds remaining in soak time
 * @param num_dots Number of dots to display in flashing ...
 */
void soakLCD(String min_time, String sec_time) {
  lcd.setCursor(5,0);
  lcd.print("SOAKING");
  lcd.setCursor(3,2);
  lcd.print(min_time);
  lcd.print(" MIN ");
  lcd.print(sec_time);
  lcd.print(" SEC");
  lcd.setCursor(5,3);
  lcd.print("REMAINING");
}

/**
 * @brief RECOVER screen
 * 
 * Displays the current position of motor recovering sample
 * 
 *  _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _
 * |_ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _|
 * |R E C O V E R I N G _ D E V I C E . . .|
 * |C U R R _ P O S I T I O N : 0 . 0 0 m _|
 * |_ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _|
 * 
 * @param position String of current position of sample device in meters
 */
void recoverLCD(String position) {
  lcd.setCursor(0,1);
  lcd.print("RECOVERING DEVICE...");
  lcd.setCursor(0,2);
  lcd.print("CURR POSITION:");
  lcd.setCursor(15,2);
  if (position.toFloat() > 15) {
    lcd.print("0.0");
  }
  else {
    lcd.print(position);
  }
  

  float rtd1 = readRTD(SAMPLE_TEMP_SENSOR);

  lcd.setCursor(0, 3);
  lcd.print("RTD1:");
  
  if (rtd1 < 10) {
    lcd.print(" ");
  }
  lcd.print(String(rtd1, 1));
  lcd.print("C");
}

/**
 * @brief SAMPLE screen
 * TODO: flash ...
 *       display state of sampling stage in Aqusens
 * 
 *  _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _
 * |_ S A M P L I N G . . . _ _ _ _ _ _ _ _|
 * |_ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _|
 * |_ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _|
 * |_ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _|
 */
void sampleLCD(unsigned long end_time) {
  lcd.setCursor(6,0);
  lcd.print("SAMPLING");
  lcd.setCursor(0,1);
  lcd.print("SEC REMAINING:");
  lcd.print("    ");
  lcd.setCursor(15, 1);
  lcd.print((int)(floor((end_time - millis()) / 1000)));
  lcd.setCursor(4, 2);
  lcd.print("RTD1: ");
  lcd.print(String(readRTD(SAMPLE_TEMP_SENSOR), 1));
  lcd.print("C");
}


/**
 * @brief LOADING SAMPLE screen
 * 
 *  _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _
 * |_ L O A D I N G   S A M P L E . . . _ _|
 * |_ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _|
 * |_ 4 5   S E C   R E M A I N I N G _ _ _|
 * |_ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _|
 */

void preSampleLCD() {
  resetLCD();


  lcd.setCursor(2, 0);
  lcd.print("LOADING SAMPLE");

  lcd.setCursor(5, 2);
  lcd.print("SEC REMAINING");

  char secString[4];
  unsigned long start_time = millis();
  unsigned long curr_time = start_time;
  unsigned long end_time = start_time + PRE_SAMPLE_LOAD_TIME_MS;
  unsigned long last_time_lcd_update_time = 0;

  int sec_remaining;

  while (curr_time < end_time) {

    if (checkEstop()) {
      setAlarmFault(ESTOP);
      pumpControl(STOP_PUMP, 0, NULL_STAGE);
      return;
    }
    checkForSerial();

    curr_time = millis();
    sec_remaining = int((end_time - curr_time) / 1000) + 1; //Rounds up, displays time 45-1 and not 44 to 0

    if (curr_time - last_time_lcd_update_time > 500) {//update every 500ms
      
      snprintf(secString, sizeof(secString), "%3d", sec_remaining);
      lcd.setCursor(1, 2);
      
      lcd.print(secString);

      lcd.setCursor(16, 0);
      printDots(sec_remaining);

      
    }
    lcd.setCursor(4, 3);
    lcd.print("RTD1: ");
    lcd.print(String(readRTD(SAMPLE_TEMP_SENSOR), 1));
    lcd.print("C");
  }

   if (detectWater()) {
        is_second_retrieval_attempt = false; //reset is_second_try to false regardless of outcome
        state = SAMPLE;
      }
      
      else if (!is_second_retrieval_attempt) { //If this is the first try to sample, then send the system back to the release state to try a second time
        is_second_retrieval_attempt = true;
        Serial.println("DID NOT DETECT WATER!! SENDING BACK TO RELEASE");
        state = RELEASE;
      }

      else if (is_second_retrieval_attempt) { //If this is the second try and we still have not detected water, then alarm
        is_second_retrieval_attempt = false;
        setAlarmFault(SAMPLE_WATER_NOT_DETECTED);
      }

  return;
}

/**
 * @brief Prints the differing amounts of dots during stages
 *        while the system waits (sampling, flushing, etc)
 * 
 *        Assumes user has moved the cursor to the correct position
 *        to print the dots
 *  _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _
 * @param sec_remaining number of seconds remaining in the current screen's stage
 */

void printDots(int sec_remaining) {
  switch (sec_remaining % 4) {
    case 0:
      lcd.print("...");
      break;
    case 1:
      lcd.print(".. ");
      break;
    case 2:
      lcd.print(".  ");
      break;
    case 3:
      lcd.print("   ");
      break;
  }
}

/**
 * @brief FLUSH_SYSTEM screen
 * 
 *  _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _
 * |_ F L U S H I N G _ S Y S T E M . . . _|
 * |_ _ _ _ _ T E M P : _ 0 0 . 0 C _ _ _ _|
 * |_ _ _ _ 0 0 _ M I N _ 0 0 _ S E C _ _ _|
 * |_ _ _ _ _ _ R E M A I N I N G _ _ _ _ _|
 * 
 * @param min_time String of minutes remaining in flush time
 * @param sec_time String of seconds remaining in flush time
 * @param num_dots Number of dots to display in flashing ...
 * @param temp_flag Flag that determines whether temperature data is displayed
 */
void flushLCD(String min_time, String sec_time, bool temp_flag, FlushStage curr_stage) {
  static FlushStage last_stage = NULL_STAGE;

  if (last_stage == NULL_STAGE || last_stage != curr_stage) {
    resetLCD();
    last_stage = curr_stage;
  }

  if (state == FLUSH_SYSTEM) {
    switch (curr_stage) {
      case DUMP_SAMPLE:
        lcd.setCursor(2,1);
        lcd.print("DUMPING SAMPLE");
        break;
      case AIR_BUBBLE:
        lcd.setCursor(5,1);
        lcd.print("AIR BUBBLE");
        break;
      case FRESHWATER_LINE_FLUSH:
        lcd.setCursor(2,1);
        lcd.print("SPRAYING LINE");
        break;
      case FRESHWATER_DEVICE_FLUSH:
        lcd.setCursor(1,1);
        lcd.print("RINSING SAMPLER");
        break;
      case AIR_FLUSH:
        lcd.setCursor(5,1);
        lcd.print("AIR FLUSH");
        break;
      case HOME_TUBE:
        lcd.setCursor(2,1);
        lcd.print("REHOMING TUBE");
        break;
      default:
        lcd.print("FLUSH STATE ERR");
        break;
    }
  }

  lcd.setCursor(1,0);
  lcd.print("FLUSHING SYSTEM");
  printDots(sec_time.toInt());

  lcd.setCursor(2, 2);
  lcd.print("FLUSH TEMP:");
  
  if (temp_flag) {
    lcd.print(String(readRTD(FLUSHWATER_TEMP_SENSOR), 1));
    lcd.print("C");
  }

  lcd.setCursor(4,3);
  lcd.print(min_time);
  lcd.print(" MIN ");
  lcd.print(sec_time);
  lcd.print(" SEC");
}

/**
 * @brief DRY screen
 * 
 * Displays the time remaining in dry procedure
 * 
 *  _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _
 * |_ _ _ _ _ _ D R Y I N G . . . _ _ _ _ _|
 * |_ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _|
 * |_ _ _ 0 0 _ M I N _ 0 0 _ S E C _ _ _ _|
 * |_ _ _ _ _ R E M A I N I N G _ _ _ _ _ _|
 * 
 * @param min_time String of minutes remaining in dry time
 * @param sec_time String of seconds remaining in dry time
 * @param num_dots Number of dots to display in flashing ...
 */
void dryLCD(String min_time, String sec_time) {
  lcd.setCursor(6,0);
  lcd.print("DRYING");
  lcd.setCursor(12, 0);


  lcd.setCursor(3,2);
  lcd.print(min_time);
  lcd.print(" MIN ");
  lcd.print(sec_time);
  lcd.print(" SEC");
  lcd.setCursor(5,3);
  lcd.print("REMAINING");
}

/**
 * @brief SOLENOID_CONTROL screen
 * 
 * Displays state of both solenoids
 * 
 *  _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _
 * |_ _ S O L E N O I D _ C O N T R O L _ _|
 * |* S O L E N O I D _ 1 : _ O P E N _ _ _|
 * |_ S O L E N O I D _ 2 : _ C L O S E D _| // TODO: UPDATE SCREEN!!
 * |< E X I T _ _ _ _ _ _ _ _ _ _ _ _ _ _ _|
 */
void solenoidControlLCD() {
  lcd.setCursor(2, 0);
  lcd.print("SOLENOID CONTROL");
  lcd.setCursor(1, 1);
  lcd.print("SV1:");
  
  if (solenoid_one_state == OPEN)
    lcd.print("ON ");
  else 
    lcd.print("OFF");

  lcd.setCursor(1, 2);
  lcd.print("SV2:");
  if (solenoid_two_state == OPEN)
    lcd.print("ON ");
  else 
    lcd.print("OFF");

  lcd.setCursor(0, cursor_y);
  lcd.print("*");


  static unsigned long lastUpdateTime = 0;
  unsigned long currentTime = millis();

  if (currentTime - lastUpdateTime >= 1000) {
    lastUpdateTime = currentTime;

    float rtd1, rtd2, rtd3;

    rtd1 = readRTD(SAMPLE_TEMP_SENSOR);
    rtd2 = readRTD(FLUSHWATER_TEMP_SENSOR);
    rtd3 = readRTD(NORA_INTERNAL_AIR_TEMP_SENSOR);

    lcd.setCursor(10, 1);
    lcd.print("RTD1:");
    
    if (rtd1 < 10) {
      lcd.print(" ");
    }
    lcd.print(String(rtd1, 1));
    lcd.setCursor(19, 1);
    lcd.print("C");

    lcd.setCursor(10, 2);
    lcd.print("RTD2:");
    
    if (rtd2 < 10) 
      lcd.print(" ");
    lcd.print(String(rtd2, 1));

    lcd.setCursor(10, 3);
    lcd.print("RTD3:");
    
    if (rtd3 < 10) 
      lcd.print(" ");
    lcd.print(String(rtd3, 1));
    lcd.print("C");

    lcd.setCursor(19, 2);
    lcd.print("C");
  }


  lcd.setCursor(0, 3);
  lcd.print("<EXIT");
}


void sendTempOverSerial() {
  float rtd1 = readRTD(SAMPLE_TEMP_SENSOR);
  float rtd2 = readRTD(FLUSHWATER_TEMP_SENSOR);
  float rtd3 = readRTD(NORA_INTERNAL_AIR_TEMP_SENSOR);
  Serial.println("RTD1: ");
  Serial.print(rtd1,2); //Print the value in Volts up to 2 decimal places
  Serial.println("C");
  Serial.println("RTD2: ");
  Serial.print(rtd2,2); //Print the value in Volts up to 2 decimal places
  Serial.println("C");
  Serial.println("RTD3: ");
  Serial.print(rtd3,2); //Print the value in Volts up to 2 decimal places
  Serial.println("C");
}
/**
 * @brief Initialize SET_CLOCK screen
 * 
 * Initialize settings page option to set clock
 */
void initSetClockLCD() {
  lcd.setCursor(6, 0);
  lcd.print("SET TIME");
  lcd.setCursor(3, 2);
  lcd.print("MO DA YR HR MN");
  lcd.setCursor(3, 3);
  lcd.print("SEL TO CONFIRM");
}

/**
 * @brief Initialize SET_INTERVAL screen
 * 
 * Initialize settings page option to set sampling interval
 */
void initSetIntervalLCD() {
  lcd.setCursor(0, 0);
  lcd.print("SET SAMPLE INTERVAL");
  lcd.setCursor(6, 2);
  lcd.print("DA HR MN");
  lcd.setCursor(3, 3);
  lcd.print("SEL TO CONFIRM");
}

/**
 * @brief Initialize SET_SOAK_TIME/SET_DRY_TIME/SET_TUBE_FLUSH_TIME/SET_AQUSENS_FLUSH_TIME screen
 * 
 * Initialize settings page option to set soak/dry/flush time
 */
void initSetSoakOrDryOrFlushLCD() {
  if (state == SET_SOAK_TIME) {
    lcd.setCursor(3, 0);
    lcd.print("SET SOAK TIME");
  }
  else if (state == SET_DRY_TIME) {
    lcd.setCursor(4, 0);
    lcd.print("SET DRY TIME");
  }

  lcd.setCursor(7, 2);
  lcd.print("MN SC");
  lcd.setCursor(3, 3);
  lcd.print("SEL TO CONFIRM");
}

/**
 * @brief Update SET_CLOCK screen
 * 
 *  Update settings page option to set clock
 * 
 * @param cursor_pos current position of the cursor on LCD within the "set clock" screen's options
 * @param adjusted_time new time to be displayed as the user updates it
 */
void updateSetClockLCD(uint8_t cursor_pos, tmElements_t adjusted_time) {
  lcd.noBlink();
  lcd.setCursor(3, 1);
  char buff[15];

  sprintf(buff, "%02d-%02d-%02d %02d:%02d", adjusted_time.Month, adjusted_time.Day, adjusted_time.Year, adjusted_time.Hour, adjusted_time.Minute);
  lcd.print(buff);

  switch (cursor_pos) {
    case 0:
      lcd.setCursor(3, 1); //Month Tens Place
      break;
    
    case 1:
      lcd.setCursor(4, 1); //Month Ones Place
      break;
    
    case 2:
      lcd.setCursor(6, 1); //Day Tens Place
      break;

    case 3:
      lcd.setCursor(7, 1); //Day Ones Place
      break;

    case 4:
      lcd.setCursor(9, 1); //Year Tens Place
      break;

    case 5:
      lcd.setCursor(10, 1); //Year Ones Place
      break;

    case 6:
      lcd.setCursor(12, 1); //Hour Tens Place
      break;

    case 7:
      lcd.setCursor(13, 1); //Hour Ones Place
      break;

    case 8:
      lcd.setCursor(15, 1); //Min Tens Place
      break;
    
    case 9:
      lcd.setCursor(16, 1); //Min Ones Place
      break;
    default:
      break;
  }
  
  lcd.blink();
}

/**
 * @brief Update SET_INTERVAL screen
 * 
 * Update settings page option to set sampling interval
 * 
 * @param cursor_pos //TODO: Jack - add a description of the parameter
 * @param adjusted_time 
 */
void updateSetIntervalLCD(uint8_t cursor_pos, tmElements_t adjusted_time) {
  lcd.noBlink();
  lcd.setCursor(6, 1);
  char buff[9];

  sprintf(buff, "%02d %02d %02d", adjusted_time.Day, adjusted_time.Hour, adjusted_time.Minute);
  lcd.print(buff);

  switch (cursor_pos) {
    case 0:
      lcd.setCursor(6, 1); //Day Tens Place
      break;
    
    case 1:
      lcd.setCursor(7, 1); //Day Ones Place
      break;
    
    case 2:
      lcd.setCursor(9, 1); //Hour Tens Place
      break;

    case 3:
      lcd.setCursor(10, 1); //Hour Ones Place
      break;

    case 4:
      lcd.setCursor(12, 1); //Min Tens Place
      break;

    case 5:
      lcd.setCursor(13, 1); //Min Ones Place
      break;
  }
  
  lcd.blink();
}

/**
 * @brief Update SET_SOAK_TIME/SET_DRY_TIME/SET_TUBE_FLUSH_TIME/SET_AQUSENS_FLUSH_TIME screen
 * 
 * Update settings page option to set soak/dry/flush time
 * 
 * @param cursor_pos //TODO: Jack - add a description of the parameter
 * @param adjusted_time 
 */
void updateSetSoakOrDryOrFlushLCD(uint8_t cursor_pos, tmElements_t adjusted_time) {
  lcd.noBlink();
  lcd.setCursor(7, 1);
  char buff[6];

  sprintf(buff, "%02d %02d", adjusted_time.Minute, adjusted_time.Second);
  lcd.print(buff);

  switch (cursor_pos) {
    case 0:
      lcd.setCursor(7, 1); //Min Tens Place
      break;
    
    case 1:
      lcd.setCursor(8, 1); //Min Ones Place
      break;
    
    case 2:
      lcd.setCursor(10, 1); //Sec Tens Place
      break;

    case 3:
      lcd.setCursor(11, 1); //Sec Ones Place
      break;
  }
  
  lcd.blink();
}

/**
 * @brief ESTOP_ALARM/MOTOR_ALARM screen
 * 
 * Displays Alarm mode for E-stop/Motor alarm
 * 
 *  _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _
 * |_ _ _ _ _ _ _ A L A R M _ _ _ _ _ _ _ _|
 * |R E A S O N : _ E - S T O P _ _ _ _ _ _|
 * |* M A N U A L _ M O D E _ _ _ _ _ _ _ _|
 * |_ E X I T _ _ _ _ _ _ _ _ _ _ _ _ _ _ _|
 */
void alarmLCD() {
  lcd.setCursor(7, 0);
  lcd.print("ALARM");
  lcd.setCursor(1, 2);
  lcd.print("MANUAL MODE");
  lcd.setCursor(1, 3);
  lcd.print("EXIT");
  lcd.setCursor(0, 1);
  lcd.print("REASON:");
  lcd.setCursor(8, 1);

  switch(fault) {
    case ESTOP:
      lcd.print("E-STOP");
      break;
    case TUBE:
      lcd.print("TUBE POS");
      break;
    case MOTOR:
      lcd.print("MOTOR");
      break;
    case TOPSIDE_COMP_COMMS:
      lcd.print("USB COMMS");
      break;
    default:
      lcd.print("IRREGULAR");
      break;
  }

  lcd.setCursor(0, cursor_y);
  lcd.print("*");
}

/**
 * @brief Release E-Stop warning screen in ESTOP_ALARM
 * 
 * Displays warning for trying to exit alarm mode without releasing E-stop
 */
void releaseEstopLCD() {
  lcd.setCursor(1, 1);
  lcd.print("RELEASE E-STOP TO");
  lcd.setCursor(2, 2);
  lcd.print("EXIT ALARM MODE");
}

/**
 * @brief MANUAL screen
 * 
 *  _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _
 * |_ _ _ _ _ M A N U A L _ M O D E _ _ _ _|
 * |* M O T O R _ _ _ _ _ _ _ _ _ _ _ _ _ _|
 * |_ S O L E N O I D S _ _ _ _ _ _ _ _ _ _|
 * |_ E X I T _ _ _ _ _ _ _ _ _ _ _ _ _ _ _|
 */
void manualLCD() {
  lcd.setCursor(5, 0);
  lcd.print("MANUAL MODE");
  lcd.setCursor(1, 1);
  lcd.print("MOTOR");
  lcd.setCursor(1, 2);
  lcd.print("SOLENOIDS");
  lcd.setCursor(1, 3);
  lcd.print("EXIT");
  
  

  
  lcd.setCursor(0, cursor_y);
  lcd.print("*");
}

/**
 * @brief MOTOR_CONTROL screen
 * 
 *  _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _
 * |_ _ _ M O T O R _ C O N T R O L S _ _ _|
 * |_ S E L _ T O _ R E S E T _ M O T O R _|
 * |^ R A I S E _ _ _ _ _ _ _ _ v L O W E R|
 * |< B A C K _ _ _ _ _ _ _ _ _ _ _ _ _ _ _|
 */
void motorControlLCD() {
  lcd.setCursor(3, 0);
  lcd.print("MOTOR CONTROLS");
  lcd.setCursor(1, 1);
  lcd.print("SEL TO RESET MOTOR");
  lcd.setCursor(0, 2);
  lcd.print("^RAISE");
  lcd.setCursor(14, 2);
  lcd.print("vLOWER");
  lcd.setCursor(0, 3);
  lcd.print("<BACK");
}

/**
 * @brief Updates display of the current position of the motor
 * 
 *  _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _
 * |_ _ _ M O T O R _ C O N T R O L S _ _ _|
 * |_ S E L _ T O _ R E S E T _ M O T O R _|
 * |^ R A I S E _ _ _ _ _ _ _ _ v L O W E R|
 * |< B A C K _ _ _ _ _ _ _ _ R A I S I N G|
 * 
 *  _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _
 * |_ _ _ M O T O R _ C O N T R O L S _ _ _|
 * |_ S E L _ T O _ R E S E T _ M O T O R _|
 * |^ R A I S E _ _ _ _ _ _ _ _ v L O W E R|
 * |< B A C K _ _ _ _ _ _ _ L O W E R I N G|
 * 
 *  _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _
 * |_ _ _ M O T O R _ C O N T R O L S _ _ _|
 * |_ S E L _ T O _ R E S E T _ M O T O R _|
 * |^ R A I S E _ _ _ _ _ _ _ _ v L O W E R|
 * |< B A C K _ _ _ _ _ _ _ _ _ _ _ _ _ _ _|
 * 
 * @param status current status of the motor (RAISING/LOWERING/OFF)
 */
void updateMotorCurrPositionDisplay(MotorDir status) {
  //int32_t currPos = 40; //Temporary, assuming will have a global variable that tracks position
  if (status != CW)
    lcd.setCursor(12, 3);
  else 
    lcd.setCursor(13, 3);

  if (status == CW)
    lcd.print("RAISING");
  else if (status == CCW)
    lcd.print("LOWERING");
  else 
    lcd.print("         ");
}

/**
 * @brief Reset Motor screen in MOTOR_CONTROL
 * TODO: remove?
 */
void resetMotorLCD() {
  lcd.setCursor(0,1);
  lcd.print("RESETTING...");
}

/**
 * @brief clears the LCD and resets cursor_y
 * 
 */
void resetLCD() {
  lcd.clear();
  cursor_y = 0;
}
