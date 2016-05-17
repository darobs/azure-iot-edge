// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

#include <stdlib.h>
#ifdef _CRTDBG_MAP_ALLOC
#include <crtdbg.h>
#endif
#include "azure_c_shared_utility/gballoc.h"

#include "dmhub.h"
#include "azure_c_shared_utility/vector.h"
#include "azure_c_shared_utility/iot_logging.h"
#include "azure_c_shared_utility/strings.h"
#include "azure_c_shared_utility/lock.h"
#include "azure_c_shared_utility/map.h"
#include "azure_c_shared_utility/condition.h"
#include "messageproperties.h"
#include "message_bus.h"

#include "iothub_lwm2m.h"
#include "azure_c_shared_utility/threadapi.h"
#include "device_object.h"
#include "firmwareupdate_object.h"
#include "config_object.h"

typedef struct PERSONALITY_TAG
{
    STRING_HANDLE deviceName;
    STRING_HANDLE deviceKey;
	IOTHUB_CHANNEL_HANDLE dmhubHandle;
    MESSAGE_BUS_HANDLE busHandle;
	THREAD_HANDLE workerThread;

}PERSONALITY;

typedef PERSONALITY* PERSONALITY_PTR;

typedef struct DMHUB_HANDLE_DATA_TAG
{
    VECTOR_HANDLE personalities; /*holds PERSONALITYs*/
    STRING_HANDLE IoTHubName;
    STRING_HANDLE IoTHubSuffix;
    MESSAGE_BUS_HANDLE busHandle;
}DMHUB_HANDLE_DATA;

#define SOURCE "source"
#define MAPPING "mapping"
#define DEVICENAME "deviceName"
#define DEVICEKEY "deviceKey"
#define DM_OPERATION "dm_operation"

// LWM2M Update State -- for /5/0/3 resource
typedef enum FIRMWARE_UPDATE_STATE_TAG
{
	LWM2M_UPDATE_STATE_NONE = 1,
	LWM2M_UPDATE_STATE_DOWNLOADING_IMAGE = 2,
	LWM2M_UPDATE_STATE_IMAGE_DOWNLOADED = 3
} LWM2M_FIRMWARE_UPDATE_STATE;

// LWM2M Update Result -- for /5/0/5 resource
typedef enum FIRMWARE_UPDATE_RESULT
{
	LWM2M_UPDATE_RESULT_DEFAULT = 0,
	LWM2M_UPDATE_RESULT_UPDATE_SUCCESSFUL = 1,
	LWM2M_UPDATE_RESULT_NOT_ENOUGH_STORAGE = 2,
	LWM2M_UPDATE_RESULT_OUT_OF_MEMORY = 3,
	LWM2M_UPDATE_RESULT_CONNECTION_LOST = 4,
	LWM2M_UPDATE_RESULT_CRC_FAILURE = 5,
	LWM2M_UPDATE_RESULT_UNSUPPORTED_PACKAGE = 6,
	LWM2M_UPDATE_RESULT_INVALID_URI = 7
} LWM2M_FIRMWARE_UPDATE_RESULT;

// App state.  This is our own construct and encapsulates update state and update result.
#define APP_UPDATE_STATE_VALUES \
    APP_UPDATE_STATE_IDLE = 0, \
    APP_UPDATE_STATE_URL_RECEIVED = 1, \
    APP_UPDATE_STATE_DOWNLOAD_IN_PROGRESS = 2, \
    APP_UPDATE_STATE_DOWNLOAD_COMPLETE = 3, \
    APP_UPDATE_STATE_UPDATE_COMMAND_RECEIVED = 4, \
    APP_UPDATE_STATE_UPDATE_IN_PROGRESS = 5, \
    APP_UPDATE_STATE_UPDATE_SUCCESSFUL = 6 
DEFINE_ENUM(APP_UPDATE_STATE, APP_UPDATE_STATE_VALUES);
DEFINE_ENUM_STRINGS(APP_UPDATE_STATE, APP_UPDATE_STATE_VALUES);

// Structure to hold the state of our simulated device
typedef struct _tagGatewayDeviceState
{
	APP_UPDATE_STATE AppUpdateState;

	PERSONALITY_PTR dmhub_personality;
	LOCK_HANDLE             dm_lock;
	COND_HANDLE             dm_cond;
} GatewayDeviceState;

GatewayDeviceState *g_gds = NULL;

static int init_gateway_globals()
{
	int rv;
	g_gds = (GatewayDeviceState*)malloc(sizeof(GatewayDeviceState));
	if (g_gds == NULL)
	{
		rv = __LINE__;
	}
	else
	{
		g_gds->dmhub_personality = NULL;
		g_gds->dm_lock = NULL;
		g_gds->dm_cond = NULL;
		rv = 0;
	}
	return rv;
}

static IOTHUB_CLIENT_RESULT start_simulated_firmware_download(object_firmwareupdate *obj)
{
	const char *uri = NULL;
	if (obj == NULL)
	{
		return IOTHUB_CLIENT_ERROR;
	}

	uri = obj->propval_firmwareupdate_packageuri;
	if (uri == NULL || *uri == 0)
	{
		LogInfo("** Empty URI received.  Resetting state machine\r\n");
		LogInfo("** %s - > APP_UPDATE_STATE_IDLE\r\n", ENUM_TO_STRING(APP_UPDATE_STATE, g_gds->AppUpdateState));
		g_gds->AppUpdateState = APP_UPDATE_STATE_IDLE;
		set_firmwareupdate_state(0, LWM2M_UPDATE_STATE_NONE);
		set_firmwareupdate_updateresult(0, LWM2M_UPDATE_RESULT_DEFAULT);
		return IOTHUB_CLIENT_OK;
	}

	else
	{
		LogInfo("** URI received from server.  Getting ready to download\r\n");
		LogInfo("** %s - > APP_UPDATE_STATE_URL_RECEIVED\r\n", ENUM_TO_STRING(APP_UPDATE_STATE, g_gds->AppUpdateState));
		g_gds->AppUpdateState = APP_UPDATE_STATE_URL_RECEIVED;
		return IOTHUB_CLIENT_OK;
	}
}


static IOTHUB_CLIENT_RESULT start_simulated_firmware_update(object_firmwareupdate *obj)
{
	LogInfo("** firmware update request posted\n");
	if (g_gds->AppUpdateState == APP_UPDATE_STATE_DOWNLOAD_COMPLETE)
	{
		LogInfo("** %s - > APP_UPDATE_STATE_UPDATE_COMMAND_RECEIVED\n", ENUM_TO_STRING(APP_UPDATE_STATE, g_gds->AppUpdateState));
		g_gds->AppUpdateState = APP_UPDATE_STATE_UPDATE_COMMAND_RECEIVED;
		return IOTHUB_CLIENT_OK;
	}

	else
	{
		LogInfo("** returning failure. Invalid state -- state = %s\n", ENUM_TO_STRING(APP_UPDATE_STATE, g_gds->AppUpdateState));
		return IOTHUB_CLIENT_INVALID_ARG;
	}
}

static IOTHUB_CLIENT_RESULT do_reboot(object_device *obj)
{
	LogInfo("** Rebooting device\r\n");
	return IOTHUB_CLIENT_OK;
}


static IOTHUB_CLIENT_RESULT do_factory_reset(object_device *obj)
{
	LogInfo("** Factory resetting device\r\n");
	return IOTHUB_CLIENT_OK;
}


static IOTHUB_CLIENT_RESULT do_apply_config(object_config *obj)
{
	LogInfo("** Applying Configuration - name = [%s] value = [%s]\r\n", obj->propval_config_name, obj->propval_config_value);
	return IOTHUB_CLIENT_OK;
}

static IOTHUB_CLIENT_RESULT send_dm_message(const char * operation_name, PERSONALITY_PTR dmhub_personality)
{
	IOTHUB_CLIENT_RESULT result;
	MESSAGE_CONFIG newMessageConfig;
	MAP_HANDLE newProperties;
	if ((newProperties = Map_Create(NULL)) == NULL)
	{
		result = IOTHUB_CLIENT_ERROR;
	}
	else
	{
		if (Map_Add(newProperties, GW_DM_OPERATION, operation_name) != MAP_OK)
		{
			Map_Destroy(newProperties);
			result = IOTHUB_CLIENT_ERROR;
		}
		else if (Map_Add(newProperties, GW_DEVICENAME_PROPERTY, STRING_c_str(dmhub_personality->deviceName)) != MAP_OK)
		{
			Map_Destroy(newProperties);
			result = IOTHUB_CLIENT_ERROR;
		}
		else if (Map_Add(newProperties, GW_SOURCE_PROPERTY, GW_DMHUB_MODULE) != MAP_OK)
		{
			Map_Destroy(newProperties);
			result = IOTHUB_CLIENT_ERROR;
		}
		else if (Map_Add(newProperties, GW_TARGET_PROPERTY, GW_IDMAP_MODULE) != MAP_OK)
		{
			Map_Destroy(newProperties);
			result = IOTHUB_CLIENT_ERROR;
		}
		else
		{
			newMessageConfig.sourceProperties = newProperties;
			newMessageConfig.size = 0;
			newMessageConfig.source = NULL;
			MESSAGE_HANDLE gatewayMsg = Message_Create(&newMessageConfig);
			if (gatewayMsg == NULL)
			{
				LogError("Failed to create gateway message");
				result = IOTHUB_CLIENT_ERROR;
			}
			else
			{
				if (MessageBus_Publish(dmhub_personality->busHandle, gatewayMsg) != MESSAGE_BUS_OK)
				{
					LogError("Failed to publish gateway message");
					result = IOTHUB_CLIENT_ERROR;
				}
				else
				{
					result = IOTHUB_CLIENT_OK;
				}
				Message_Destroy(gatewayMsg);
			}

		}
			 
	}
	return result;
}

static IOTHUB_CLIENT_RESULT do_manufacturer_read(object_config *obj)
{
	IOTHUB_CLIENT_RESULT result;
	if (Lock(g_gds->dm_lock) != LOCK_OK)
	{
		LogError("unable to lock");
		result = IOTHUB_CLIENT_ERROR;
	}
	else
	{
		/* Step 1, send message to get data*/
		result = send_dm_message("manufacturer_read", g_gds->dmhub_personality);

		if (result == IOTHUB_CLIENT_OK)
		{
			/* Step 2, wait on condition (timed wait) */

			if ((Condition_Wait(g_gds->dm_cond, g_gds->dm_lock, 5000) == COND_OK))
			{
				/* return success value of timed wait */
				result = IOTHUB_CLIENT_OK;
			}
			else
			{
				result = IOTHUB_CLIENT_ERROR;
			}
		}
		Unlock(g_gds->dm_lock);
	}
	return result;
}

static IOTHUB_CLIENT_RESULT do_modelnumber_read(object_config *obj)
{
	IOTHUB_CLIENT_RESULT result;
	if (Lock(g_gds->dm_lock) != LOCK_OK)
	{
		LogError("unable to lock");
		result = IOTHUB_CLIENT_ERROR;
	}
	else
	{
		/* Step 1, send message to get data*/
		result = send_dm_message("modelnumber_read", g_gds->dmhub_personality);

		if (result == IOTHUB_CLIENT_OK)
		{
			/* Step 2, wait on condition (timed wait) */

			if ((Condition_Wait(g_gds->dm_cond, g_gds->dm_lock, 5000) == COND_OK))
			{
				/* return success value of timed wait */
				result = IOTHUB_CLIENT_OK;
			}
			else
			{
				result = IOTHUB_CLIENT_ERROR;
			}
		}
		Unlock(g_gds->dm_lock);
	}
	return result;
}

static IOTHUB_CLIENT_RESULT do_serialnumber_read(object_config *obj)
{
	IOTHUB_CLIENT_RESULT result;
	if (Lock(g_gds->dm_lock) != LOCK_OK)
	{
		LogError("unable to lock");
		result = IOTHUB_CLIENT_ERROR;
	}
	else
	{
		/* Step 1, send message to get data*/
		result = send_dm_message("serialnumber_read", g_gds->dmhub_personality);

		if (result == IOTHUB_CLIENT_OK)
		{
			/* Step 2, wait on condition (timed wait) */

			if ((Condition_Wait(g_gds->dm_cond, g_gds->dm_lock, 5000) == COND_OK))
			{
				/* return success value of timed wait */
				result = IOTHUB_CLIENT_OK;
			}
			else
			{
				result = IOTHUB_CLIENT_ERROR;
			}
		}
		Unlock(g_gds->dm_lock);
	}
	return result;
}

static IOTHUB_CLIENT_RESULT do_firmwareversion_read(object_config *obj)
{
	IOTHUB_CLIENT_RESULT result;
	if (Lock(g_gds->dm_lock) != LOCK_OK)
	{
		LogError("unable to lock");
		result = IOTHUB_CLIENT_ERROR;
	}
	else
	{
		/* Step 1, send message to get data*/
		result = send_dm_message("firmwareversion_read", g_gds->dmhub_personality);

		if (result == IOTHUB_CLIENT_OK)
		{
			/* Step 2, wait on condition (timed wait) */

			if ((Condition_Wait(g_gds->dm_cond, g_gds->dm_lock, 5000) == COND_OK))
			{
				/* return success value of timed wait */
				result = IOTHUB_CLIENT_OK;
			}
			else
			{
				result = IOTHUB_CLIENT_ERROR;
			}
		}
		Unlock(g_gds->dm_lock);
	}
	return result;
}

void set_initial_property_state()
{
	set_firmwareupdate_state(0, LWM2M_UPDATE_STATE_NONE);
	set_firmwareupdate_updateresult(0, LWM2M_UPDATE_RESULT_DEFAULT);
}

static void dmhub_set_callbacks()
{
	object_firmwareupdate *f_obj = get_firmwareupdate_object(0);
	f_obj->firmwareupdate_packageuri_write_callback = start_simulated_firmware_download;
	f_obj->firmwareupdate_update_execute_callback = start_simulated_firmware_update;

	object_device*d_obj = get_device_object(0);
	d_obj->device_reboot_execute_callback = do_reboot;
	d_obj->device_factoryreset_execute_callback = do_factory_reset;
	d_obj->device_manufacturer_read_callback = do_manufacturer_read;
	d_obj->device_modelnumber_read_callback = do_modelnumber_read;
	d_obj->device_serialnumber_read_callback = do_serialnumber_read;
	d_obj->device_firmwareversion_read_callback = do_firmwareversion_read;

	object_config *c_obj = get_config_object(0);
	c_obj->config_apply_execute_callback = do_apply_config;

}

static int set_gateway_globals()
{
	int rv;
	if ((g_gds->dm_lock = Lock_Init()) == NULL)
	{
		rv = __LINE__;
	}
	else
	{
		if ((g_gds->dm_cond = Condition_Init()) == NULL)
		{
			Lock_Deinit(g_gds->dm_lock);
			rv = __LINE__;
		}	
		else
		{
			set_initial_property_state();
			dmhub_set_callbacks();
			rv = 0;
		}
	}
	return rv;
}

int dmhub_work(void* context)
{
	int thrResult;
	PERSONALITY_PTR dmPersonality = (PERSONALITY_PTR)context;
	if (IOTHUB_CLIENT_OK != IoTHubClient_DM_Start(dmPersonality->dmhubHandle))
	{
		LogError("DM Client work did not start");
		thrResult = -1;
	}
	else
	{
		thrResult = 0;
	}
	return thrResult;
}

static MODULE_HANDLE dmhub_Create(MESSAGE_BUS_HANDLE busHandle, const void* configuration)
{
    DMHUB_HANDLE_DATA *result;
    if (
        (busHandle == NULL) ||
        (configuration == NULL) ||
		(((const DMHUB_CONFIG*)configuration)->IoTHubName == NULL) ||
		(((const DMHUB_CONFIG*)configuration)->IoTHubSuffix == NULL)
        )
    {
        LogError("invalid arg busHandle=%p, configuration=%p, IoTHubName=%s IoTHubSuffix=%s ", busHandle, configuration, (configuration!=NULL)?((const DMHUB_CONFIG*)configuration)->IoTHubName:"undefined behavior", (configuration != NULL) ? ((const DMHUB_CONFIG*)configuration)->IoTHubSuffix : "undefined behavior");
        result = NULL;
    }
    else
    {
        result = malloc(sizeof(DMHUB_HANDLE_DATA));
        if (result == NULL)
        {
            LogError("malloc returned NULL");
            /*return as is*/
        }
        else
        {
            result->personalities = VECTOR_create(sizeof(PERSONALITY_PTR));
            if (result->personalities == NULL)
            {
                free(result);
                result = NULL;
                LogError("VECTOR_create returned NULL");
            }
            else
            {
				if ((result->IoTHubName = STRING_construct(((const DMHUB_CONFIG*)configuration)->IoTHubName)) == NULL)
				{
					VECTOR_destroy(result->personalities);
					free(result);
					result = NULL;
				}
				else if ((result->IoTHubSuffix = STRING_construct(((const DMHUB_CONFIG*)configuration)->IoTHubSuffix)) == NULL)
				{
					STRING_delete(result->IoTHubName);
					VECTOR_destroy(result->personalities);
					free(result);
					result = NULL;
				}
				else
				{
					if (init_gateway_globals() != 0)
					{
						STRING_delete(result->IoTHubSuffix);
						STRING_delete(result->IoTHubName);
						VECTOR_destroy(result->personalities);
						free(result);
						result = NULL;
					}
					else
					{
						result->busHandle = busHandle;
					}
				}	
            }
        }
    }
    return result;
}

static void dmhub_Destroy(MODULE_HANDLE moduleHandle)
{
    if (moduleHandle == NULL)
    {
        LogError("moduleHandle parameter was NULL");
    }
    else
    {
        DMHUB_HANDLE_DATA * handleData = moduleHandle;
        size_t vectorSize = VECTOR_size(handleData->personalities);
        for (size_t i = 0; i < vectorSize; i++)
        {
            PERSONALITY_PTR* personality = VECTOR_element(handleData->personalities, i);
			/* join and free thread */
			IoTHubClient_DM_Close((*personality)->dmhubHandle);
			STRING_delete((*personality)->deviceKey);
            STRING_delete((*personality)->deviceName);
            free(*personality);
        }
		VECTOR_destroy(handleData->personalities);
        STRING_delete(handleData->IoTHubName);
        STRING_delete(handleData->IoTHubSuffix);
        free(handleData);
    }
}

static bool lookup_DeviceName(const void* element, const void* value)
{
    return (strcmp(STRING_c_str((*(PERSONALITY_PTR*)element)->deviceName), value) == 0);
}


/*returns non-null if PERSONALITY has been properly populated*/
static PERSONALITY_PTR PERSONALITY_create(const char* deviceName, const char* deviceKey, DMHUB_HANDLE_DATA* moduleHandleData)
{
    PERSONALITY_PTR result = (PERSONALITY_PTR)malloc(sizeof(PERSONALITY));
    if (result == NULL)
    {
        LogError("unable to allocate a personality for the device %s", deviceName);
    }
    else
    {
        if ((result->deviceName = STRING_construct(deviceName)) == NULL)
        {
            LogError("unable to STRING_construct");
                free(result);
                result = NULL;
        }
        else if ((result->deviceKey = STRING_construct(deviceKey)) == NULL)
        {
            LogError("unable to STRING_construct");
                STRING_delete(result->deviceName);
                free(result);
                result = NULL;
        }
        else
        {
			/* construct a connection string */
			/* Format: HostName=<IoTHubName>.<IoTHubSuffix>;DeviceId=<deviceName>;SharedAccessKey=<deviceKey> */
			STRING_HANDLE cs = STRING_new();
			STRING_concat(cs, "HostName=");
			STRING_concat_with_STRING(cs, moduleHandleData->IoTHubName);
			STRING_concat(cs, ".");
			STRING_concat_with_STRING(cs, moduleHandleData->IoTHubSuffix);
			STRING_concat(cs, ";DeviceId=");
			STRING_concat(cs, deviceName);
			STRING_concat(cs, ";SharedAccessKey=");
			STRING_concat(cs, deviceKey);
			if (cs == NULL)
			{
				STRING_delete(result->deviceKey);
				STRING_delete(result->deviceName);
				free(result);
			}
			else
			{
				//    IOTHUB_CHANNEL_HANDLE IoTHubChannel = IoTHubClient_DM_Open(cs, COAP_TCPIP);
				if ((result->dmhubHandle = IoTHubClient_DM_Open(STRING_c_str(cs), COAP_TCPIP)) == NULL)
				{
					LogError("unable to IoTHubClient_DM_Open");
					STRING_delete(result->deviceName);
					STRING_delete(result->deviceKey);
					free(result);
					result = NULL;
				}
				else
				{
					if (IOTHUB_CLIENT_OK != IoTHubClient_DM_CreateDefaultObjects(result->dmhubHandle))
					{
						LogError("failure to create LWM2M objects for client: %p\r\n", result->dmhubHandle);
						IoTHubClient_DM_Close(result->dmhubHandle);

						STRING_delete(result->deviceName);
						STRING_delete(result->deviceKey);
						free(result);
						result = NULL;
					}
					else
					{
						THREADAPI_RESULT tr = ThreadAPI_Create(&(result->workerThread), &dmhub_work, result);
						if (tr != THREADAPI_OK)
						{
							LogError("failed to create background thread, error=%d", tr);
							IoTHubClient_DM_Close(result->dmhubHandle);

							STRING_delete(result->deviceName);
							STRING_delete(result->deviceKey);
							free(result);
							result = NULL;
						}
						else
						{
							if (set_gateway_globals() != 0)
							{
								IoTHubClient_DM_Close(result->dmhubHandle);

								STRING_delete(result->deviceName);
								STRING_delete(result->deviceKey);
								free(result);
								result = NULL;
							}
							else
							{
								/*it is all fine*/
								result->busHandle = moduleHandleData->busHandle;
							}

						}
						
					}
				}
				STRING_delete(cs);
			}
        }
    }
    return result;
}

static void PERSONALITY_destroy(PERSONALITY* personality)
{
	IoTHubClient_DM_Close(personality->dmhubHandle);
	STRING_delete(personality->deviceName);
    STRING_delete(personality->deviceKey);
}

static PERSONALITY* PERSONALITY_find_or_create(DMHUB_HANDLE_DATA* moduleHandleData, const char* deviceName, const char* deviceKey)
{
    PERSONALITY* result;
    PERSONALITY_PTR* resultPtr = VECTOR_find_if(moduleHandleData->personalities, lookup_DeviceName, deviceName);
	/* Can only have one DMhub in preview */
    if ((resultPtr == NULL) && (VECTOR_size(moduleHandleData->personalities) < 1))
    {
        /*a new device has arrived!*/
        PERSONALITY_PTR personality;
        if ((personality = PERSONALITY_create(deviceName, deviceKey, moduleHandleData)) == NULL)
        {
            LogError("unable to create a personality for the device %s", deviceName);
            result = NULL;
        }
        else
        {
            if ((VECTOR_push_back(moduleHandleData->personalities, &personality, 1)) != 0)
            {
                LogError("VECTOR_push_back failed");
                PERSONALITY_destroy(personality);
                free(personality);
                result = NULL;
            }
            else
            {
                resultPtr = VECTOR_back(moduleHandleData->personalities);
                result = *resultPtr;
            }
            }
        }
    else
    {
        result = *resultPtr;
    }
	g_gds->dmhub_personality = result;
    return result;
}

static IOTHUB_MESSAGE_HANDLE IoTHubMessage_CreateFromGWMessage(MESSAGE_HANDLE message)
{
    IOTHUB_MESSAGE_HANDLE result;
    const CONSTBUFFER* content = Message_GetContent(message);
    result = IoTHubMessage_CreateFromByteArray(content->buffer, content->size);
    if (result == NULL)
    {
        LogError("IoTHubMessage_CreateFromByteArray failed");
        /*return as is*/
    }
    else
    {
        MAP_HANDLE iothubMessageProperties = IoTHubMessage_Properties(result);
        CONSTMAP_HANDLE gwMessageProperties = Message_GetProperties(message);
        const char* const* keys;
        const char* const* values;
        size_t nProperties;
        if (ConstMap_GetInternals(gwMessageProperties, &keys, &values, &nProperties) != CONSTMAP_OK)
        {
            LogError("unable to get properties of the GW message");
            IoTHubMessage_Destroy(result);
            result = NULL;
        }
        else
        {
            size_t i;
            for (i = 0; i < nProperties; i++)
            {
                /*add all the properties of the GW message to the IOTHUB message*/ /*with the exception*/
                if (
                    (strcmp(keys[i], "deviceName") != 0) &&
                    (strcmp(keys[i], "deviceKey") != 0)
                    )
                {
                   
                    if (Map_AddOrUpdate(iothubMessageProperties, keys[i], values[i]) != MAP_OK)
                    {
                        LogError("unable to Map_AddOrUpdate");
                        break;
                    }
                }
            }

            if (i == nProperties)
            {
                /*all is fine, return as is*/
            }
            else
            {
                IoTHubMessage_Destroy(result);
                result = NULL;
            }
        }
        ConstMap_Destroy(gwMessageProperties);
    }
    return result;
}

static void update_manufacturer(PERSONALITY* personality, const CONSTBUFFER *message)
{
	char * newString = (char*)malloc(message->size +1);
	if (newString != NULL)
	{
		strncpy(newString, message->buffer, message->size);
		newString[message->size] = '\0';
		set_device_manufacturer(0, newString);
		Condition_Post(g_gds->dm_cond);
		free(newString);
	}
}

static void update_modelnumber(PERSONALITY* personality, const CONSTBUFFER *message)
{
	char * newString = (char*)malloc(message->size + 1);
	if (newString != NULL)
	{
		strncpy(newString, message->buffer, message->size);
		newString[message->size] = '\0';
		set_device_modelnumber(0, newString);
		Condition_Post(g_gds->dm_cond);
		free(newString);
	}
}

static void update_serialnumber(PERSONALITY* personality, const CONSTBUFFER *message)
{
	char * newString = (char*)malloc(message->size + 1);
	if (newString != NULL)
	{
		strncpy(newString, message->buffer, message->size);
		newString[message->size] = '\0';
		set_device_serialnumber(0, newString);
		Condition_Post(g_gds->dm_cond);
		free(newString);
	}
}

static void update_firmwareversion(PERSONALITY* personality, const CONSTBUFFER *message)
{
	char * newString = (char*)malloc(message->size + 1);
	if (newString != NULL)
	{
		strncpy(newString, message->buffer, message->size);
		newString[message->size] = '\0';
		set_device_firmwareversion(0, newString);
		Condition_Post(g_gds->dm_cond);
		free(newString);
	}
}
static void process_dm_message(const char * dm_operation, PERSONALITY* personality, const CONSTBUFFER *message)
{
	if (!(dm_operation == NULL || personality == NULL || message == NULL))
	{
		if (Lock(g_gds->dm_lock) == LOCK_OK)
		{

			if (strcmp(dm_operation, "manufacturer_read") == 0)
			{
				update_manufacturer(personality, message);
			}
			else if (strcmp(dm_operation, "modelnumber_read") == 0)
			{
				update_modelnumber(personality, message);
			}
			else if (strcmp(dm_operation, "serialnumber_read") == 0)
			{
				update_serialnumber(personality, message);
			}
			else if (strcmp(dm_operation, "firmwareversion_read") == 0)
			{
				update_firmwareversion(personality, message);
			}

			Unlock(g_gds->dm_lock);
		}
	}
}

static void dmhub_Receive(MODULE_HANDLE moduleHandle, MESSAGE_HANDLE messageHandle)
{
    if (
        (moduleHandle == NULL) ||
        (messageHandle == NULL)
        )
    {
        LogError("invalid arg moduleHandle=%p, messageHandle=%p", moduleHandle, messageHandle);
        /*do nothing*/
    }
    else
    {
        CONSTMAP_HANDLE properties = Message_GetProperties(messageHandle);
        const char* source = ConstMap_GetValue(properties, SOURCE); /*properties is !=NULL by contract of Message*/
		const char * target = ConstMap_GetValue(properties, GW_TARGET_PROPERTY);
        if (
            (source == NULL) ||
			(target == NULL) ||
			(strcmp(source, MAPPING) != 0) ||
			(strcmp(target, GW_DMHUB_MODULE)!=0)
            )
        {
            /*do nothing, the properties do not contain either "source" or "source":"mapping"*/
        }
        else
        {
            const char* deviceName = ConstMap_GetValue(properties, DEVICENAME);
            if (deviceName == NULL)
            {
                /*do nothing, not a message for this module*/
            }
            else
            {
                const char* deviceKey = ConstMap_GetValue(properties, DEVICEKEY);
                if (deviceKey == NULL)
                {
                    /*do nothing, missing device key*/
                }
                else
                {
                    DMHUB_HANDLE_DATA* moduleHandleData = moduleHandle;
                    
                    PERSONALITY* whereIsIt = PERSONALITY_find_or_create(moduleHandleData, deviceName, deviceKey);
                    if (whereIsIt == NULL)
                    {
                        /*do nothing, device was not added to the GW*/
                        LogError("unable to PERSONALITY_find_or_create");
                    }
                    else
                    {
						process_dm_message(ConstMap_GetValue(properties, DM_OPERATION), whereIsIt, Message_GetContent(messageHandle));
                    }
                }
            }
        }
        ConstMap_Destroy(properties);
    }
}

static const MODULE_APIS Module_GetAPIS_Impl = 
{
    dmhub_Create,
    dmhub_Destroy,
    dmhub_Receive
};

#ifdef BUILD_MODULE_TYPE_STATIC
MODULE_EXPORT const MODULE_APIS* MODULE_STATIC_GETAPIS(DMHUB_MODULE)(void)
#else
MODULE_EXPORT const MODULE_APIS* Module_GetAPIS(void)
#endif
{
    return &Module_GetAPIS_Impl;
}