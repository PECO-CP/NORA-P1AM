#ifndef CONFIG_H
#define CONFIG_H

// Macros for converting total pulses to cm and vice-versa 
#define PULSES_TO_DISTANCE(num_pulses) ((num_pulses * PI * 2 * REEL_RADIUS_CM)/(PULSES_PER_REV * GEARBOX_RATIO))
#define DISTANCE_TO_PULSES(distance_cm) ((GEARBOX_RATIO * PULSES_PER_REV * distance_cm) / (PI * 2 * REEL_RADIUS_CM))


/************************* Python Script Comms *************************/
#define STOP_PUMP         "F" // Requests Aqusens pump to begin
#define START_PUMP        "P" // Request Aqusens pump to stop
#define BEGIN_SAMPLE      "S" // Requests sample to start
#define REQUEST_TIME      "C" // Requests the current time in unix-epoch form
#define REQUEST_TIDE_DATA "T" // Requests current tide level from NOAA API

#define COMMS_REPORT_MOTOR_ERR                      "EM"  // Report motor alarm error
#define COMMS_REPORT_TUBE_ERR                       "ET"  // Report tube retrieval error
#define COMMS_REPORT_SAMPLE_WATER_NOT_DETECTED_ERR  "EW"  // Report sample water not detected error
#define COMMS_REPORT_ESTOP_PRESSED                  "EE"  // Report that the e-stop has been pressed
#define COMMS_REPORT_SAMPLE_BEGIN                   "ES"  // Report that a sample has begun
#define COMMS_REPORT_SAMPLE_END                     "EC"  //Report that sample is over!

/************************* Default Timings *************************/
#define DEFAULT_SAMPLE_INTERVAL_HOUR 8
#define DEFAULT_SAMPLE_INTERVAL_MIN  0
#define DEFAULT_SAMPLE_INTERVAL_SEC  0

#define DEFAULT_SOAK_TIME_MIN        0
#define DEFAULT_SOAK_TIME_SEC        15

#define DEFAULT_DRY_TIME_MIN         0
#define DEFAULT_DRY_TIME_SEC         5

// #define SAMPLE_TIME_SEC              60*6 //Sample for 6 minutes
#define SAMPLE_TIME_SEC 3 * 60
#define PRE_SAMPLE_LOAD_TIME_MS 100 * 1000

/************************* File & Time Config *************************/
#define TIDE_FILE_NAME    "tides.txt"
#define GMT_TO_PST        (8)

/************************* Motor Configuration (motor.ino) *************************/
#define REEL_RADIUS_CM        (5.0f)  //Radius of the line reel
#define PULSES_PER_REV        (1600)  // Num of pulses for each STEPPER rotation 
#define GEARBOX_RATIO         (5.0f)  // Gear ratio of the current gearbox, 5:1 currently

#define SYSTEM_CLOCK_FREQ       (48000000)
#define PRESCALER_VAL           (8)

#define ALARM_THRESHOLD_VALUE   (1100) // Analog value on the Alarm Plus pin that seperates alarm state from non-alarm state.

/************************* Debounce Timings (utils.ino) *************************/
#define TIME_BASED_DEBOUNCE_WAIT_TIME_MS (35)
#define PRESS_AND_HOLD_INTERVAL_MS       (25)

/************************* Positioning Parameters (position.ino) *************************/
#define NARROW_TUBE_CM              (15.0f) 
#define TUBE_CM                     (85.0f) 
#define WATER_LEVEL_CM              (15.0f) 
#define DROP_SPEED_CM_SEC           (75.0f)
#define RAISE_SPEED_CM_SEC          (50.0f)
#define SAFE_RISE_SPEED_CM_SEC      (3.0f)
#define TUBE_DROP_OVERSHOOT         (30)
#define NEAR_WATER_SPEED_CM_SEC     (10.0) //Speed for when we near the water on the way up and down when sampling
#define IN_TUBE_RAISE_SPEED_CM_SEC  (10.0f) //Raise speed once we're in the tube but not year near home

#define NEAR_WATER_DIST_CM          (100.0) //Distance from the water at which we slowly raise/lower the tube
#define ALIGNMENT_TUBE_OPENING_DIST (208 + 20) // 208cm long "alignment tube", plus 20cm of buffer
#define NEARING_HOME_DIST           (30) // slow down to a crawl at 15 cm from the magnet

#define MOTOR_STEP_DELAY_MS         (50)

#define TUBE_TIMEOUT_ERR_TIME_MS    (200 * 1000) // If we go 200 seconds in the retrieve stage without detecting the sampler, something's gone wrong

#define DRAIN_LIFT_SPEED_CM_S       (2.0f)
#define DRAIN_HOME_SPEED_CM_S       (-2.0f)

#define MANUAL_CONTROL_MOTOR_SPEED  (20.0f)

#define SECOND_ATTEMPT_DROP_DIST_INCREASE_CM 100

/************************* Tube Flush Timings & Thresholds (tube_flush.ino) *************************/
// General timings
#define LIFT_TUBE_TIME_S              (10.0f)
#define AIR_BUBBLE_TIME_S             (90.0f)
#define SAMPLE_TO_DEVICE_TIME_S       (90.0f)
#define FLUSH_LINE_TIME_S               (5.0f) //Get this experimentally based on how far we send the line down to flush
#define FRESHWATER_TO_DEVICE_TIME_S   (35.0f)
#define FRESHWATER_FLUSH_TIME_S       (150.0f)
#define FINAL_AIR_FLUSH_TIME_S        (150.0f)
#define FLUSHING_TIME_BUFFER          (10.0f)  //Gives some extra time to each of the steps in flush, ensures proper flushing
#define DEVICE_FLUSH_WATER_TEMP_MAX_C (27.0f) // Change this to the actual value, I assume the max somewhere south of boiling

////////////////////? TEST VALUES FOR DEBUGGING, REAL VALUES ABOVE
// #define LIFT_TUBE_TIME_S              (5.0f)
// #define AIR_BUBBLE_TIME_S             (10.0f)
// #define SAMPLE_TO_DEVICE_TIME_S       (10.0f)
// #define FLUSH_LINE_TIME_S               (5.0f) //Get this experimentally based on how far we send the line down to flush
// #define FRESHWATER_TO_DEVICE_TIME_S   (10.0f)
// #define FRESHWATER_FLUSH_TIME_S       (10.0f)
// #define FINAL_AIR_FLUSH_TIME_S        (10.0f)
// #define FLUSHING_TIME_BUFFER          (10.0f)  //Gives some extra time to each of the steps in flush, ensures proper flushing
// #define DEVICE_FLUSH_WATER_TEMP_MAX_C (100.0f) // Change this to the actual value, I assume the max somewhere south of boiling

#define LINE_FLUSH_DROP_DIST_CM (60.0f)
#define HOME_TUBE_SPD_CM_S      (3.0f)

/************************* Water Detection (utils.ino) ******************/
#define WATER_DETECTION_TIMEOUT_MS                10 * 1000
#define MINIMUM_TEMP_DELTA_FOR_WATER_DETECTION_C (2.0f)

/************************* SD Storage (sd.ino) *************************/
#define PIER_DEFAULT_DIST_CM    (762.0f)

/************************* Pin Mapping *************************/
// Slots - Position of modules
#define HV_GPIO_SLOT 1
#define RELAY_SLOT   2
#define RTD_SLOT     3

// Keypad (GPIO)
#define KEY_S 0
#define KEY_D 1
#define KEY_U 2
#define KEY_L 3
#define KEY_R 4

// SD Card
#define SD_CS 28

// Motor (GPIO)
#define STEP_POS_PIN       6
#define DIR_POS_PIN        13
#define ALARM_PLUS         A2
#define ALARM_MINUS        A5

// HV GPIO
#define MAG_SENSOR_IO_SLOT 1

// Relays
#define MOTOR_POWER   1
#define SOLENOID_ONE  3
#define SOLENOID_TWO  4

// Misc
#define TOPSIDE_COMP_COMMS_TIMEOUT_MS 30 * 1000

/************************* Temperature Sensors *************************/
typedef enum TempSensor {
  SAMPLE_TEMP_SENSOR            = 1,
  FLUSHWATER_TEMP_SENSOR        = 2,
  NORA_INTERNAL_AIR_TEMP_SENSOR = 3
} TempSensor;

/************************* State and Fault Enums *************************/
// Machine States
enum StateEnum {
  CALIBRATE,
  STANDBY,
  RELEASE,
  SOAK,
  RECOVER,
  SAMPLE,
  FLUSH_SYSTEM,
  DRY,
  ALARM,
  MANUAL,
  MOTOR_CONTROL,
  SOLENOID_CONTROL,
  SETTINGS,
  SET_INTERVAL,
  ENSURE_SAMPLE_START,
  SET_START_TIME,
  SET_CLOCK,
  SET_DRY_TIME,
  SET_SOAK_TIME,
  FILTER_STATUS
};

// Faults
typedef enum AlarmFault { 
  NONE,
  MOTOR,
  TUBE,
  ESTOP,
  TOPSIDE_COMP_COMMS,
  SAMPLE_WATER_NOT_DETECTED
} AlarmFault;

// Stages for the flushing procedure
typedef enum FlushStage { 
  NULL_STAGE                = -1,
  DUMP_SAMPLE               =  0,
  AIR_BUBBLE                =  1,
  FRESHWATER_LINE_FLUSH     =  2,
  FRESHWATER_DEVICE_FLUSH   =  3,
  AIR_FLUSH                 =  4,
  HOME_TUBE                 =  5
} FlushStage;


enum RetrieveStage {
  INITIAL_SLOW_RISE,
  NORMAL_RISE,
  STOP_AND_WAIT,
  SLOW_RISE_TO_NEAR_HOME,
  FINAL_SLOW_ALIGN,
  RETRIEVAL_COMPLETE
};

typedef enum MotorDir {
  CCW, //Lowering
  CW,  //Raising
  OFF
} MotorDir;


#endif
