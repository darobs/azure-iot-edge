// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

#include <stdlib.h>
#ifdef _CRTDBG_MAP_ALLOC
#include <crtdbg.h>
#endif

#include "azure_c_shared_utility/crt_abstractions.h"
#include "azure_c_shared_utility/vector.h"
#include "azure_c_shared_utility/iot_logging.h"
#include "azure_c_shared_utility/vector.h"
#include "azure_c_shared_utility/map.h"
#include "message.h"
#include "parson.h"
#include "messageproperties.h"

#include "dm_worker.h"

#define MACADDR "macAddress"
#define DEVICENAME "deviceId"
#define DEVICEKEY "deviceKey"
#define DM_OPERATION "dm_operation"

/*
 * configuration for this module needs to map DM operation to GATT characteristic instruction.
 {
 "id" : <id>",
 "<dm function1>" :  {
			 "type" : "<instruction type>",
			 "characteristic_uuid": "<uuid>",
			 ....
		 }
 "<dm function2> :{
			"type" : "<instruction type>",
			"characteristic_uuid": "<uuid>",
			....
		}
 }
 */

typedef struct DM_WORKER_DATA_TAG
{
	const char * identification;
	JSON_Value* config;
	JSON_Object* root;
	MESSAGE_BUS_HANDLE busHandle;
} DM_WORKER_DATA;

/*
* @brief	duplicate a string and convert to upper case. New string must be released
*			no longer needed.
*/
static const char * duplicate_upper_case(const char * string)
{
	char * result;
	int status;
	status = mallocAndStrcpy_s(&result, string);
	if (status != 0) // failure
	{
		result = NULL;
	}
	else
	{
		char * temp = result;
		while (*temp)
		{
			*temp = toupper(*temp);
			temp++;
		}
	}

	return result;
}


/*
 * @brief	Create an identity map HL module.
 */
static MODULE_HANDLE DMWorker_Create(MESSAGE_BUS_HANDLE busHandle, const void* configuration)
{
	DM_WORKER_DATA *result;
	if ((busHandle == NULL) || (configuration == NULL))
	{
		LogError("Invalid NULL parameter, busHandle=[%p] configuration=[%p]", busHandle, configuration);
		result = NULL;
	}
	else
	{
		JSON_Value* json = json_parse_string((const char*)configuration);
		if (json == NULL)
		{
			LogError("Unable to parse json string");
			result = NULL;
		}
		else
		{
			JSON_Object* root = json_value_get_object(json);
			if (root == NULL)
			{
				LogError("unable to json_value_get_object");
				result = NULL;
			}
			else
			{
				// get controller index
				const char * id = json_object_get_string(root, "id");
				if (id == NULL)
				{
					json_value_free(json);
					LogError("unable to json_value_get_object");
					result = NULL;
				}
				else
				{
					result = (DM_WORKER_DATA*)malloc(sizeof(DM_WORKER_DATA));
					if (result == NULL)
					{
						json_value_free(json);
						LogError("unable to allocate data for module");
					}
					else
					{
						result->identification = duplicate_upper_case(id);
						if (result->identification == NULL)
						{
							json_value_free(json);
							LogError("unable to allocate id string for module");
							free(result);
							result = NULL;
						}
						else
						{
							result->config = json;
							result->root = root;
							result->busHandle = busHandle;
						}
					}
				}

			}
		}
	}
	return result;
}

/*
* @brief	Destroy an identity map HL module.
*/
static void DMWorker_Destroy(MODULE_HANDLE moduleHandle)
{
	if (moduleHandle != NULL)
	{
		DM_WORKER_DATA * moduleData = (DM_WORKER_DATA*)moduleHandle;
		free(moduleData->identification);
		json_value_free(moduleData->config);
		free(moduleData);
	}
}

static void publish_device_message(DM_WORKER_DATA * moduleData, const char * msgOperation, CONSTMAP_HANDLE properties)
{
	JSON_Object* root = moduleData->root;
	JSON_Object * deviceOperation = json_object_get_object(root, msgOperation);
	if (deviceOperation != NULL)
	{
		char * serialized = json_serialize_to_string(deviceOperation);
		if (serialized != NULL)
		{
			MAP_HANDLE newProperties = ConstMap_CloneWriteable(properties);
			if (newProperties != NULL)
			{
				if (Map_AddOrUpdate(newProperties, GW_SOURCE_PROPERTY, GW_WORKER_MODULE) == MAP_OK)
				{
					MESSAGE_CONFIG newMessage;

					newMessage.size = strlen(serialized);
					newMessage.source = (const unsigned char*)serialized;
					newMessage.sourceProperties = newProperties;
					MESSAGE_HANDLE newBusMessage = Message_Create(&newMessage);
					if (newBusMessage != NULL)
					{
						MessageBus_Publish(moduleData->busHandle, newBusMessage);
					}
					else
					{
						LogError("Failed to create a new message");
					}
				}
				Map_Destroy(newProperties);
			}
			else
			{
				LogError("failed to create a new properties map");
			}


			json_free_serialized_string(serialized);

		}
	}

}

static void publish_dm_response(DM_WORKER_DATA * moduleData, const char * msgCharacteristicUuid, MESSAGE_HANDLE messageHandle)
{
	JSON_Object* root = moduleData->root;
	size_t objectCount =   json_object_get_count(root);
	size_t curObject;
	for (curObject = 0; curObject < objectCount; curObject++)
	{
		const char * curName = json_object_get_name(root, curObject);
		if (curName != NULL)
		{
			JSON_Value * curValue = json_object_get_value_at(root, curObject);
			JSON_Object * curObj = json_value_get_object(curValue);
			if (curObj != NULL)
			{
				const char  * curCharUUID = json_object_get_string(curObj, "characteristic_uuid");
				if (curCharUUID != NULL && strcmp(curCharUUID, msgCharacteristicUuid) == 0)
				{
					CONSTMAP_HANDLE properties = Message_GetProperties(messageHandle);

					MAP_HANDLE newProperties = ConstMap_CloneWriteable(properties);
					if (newProperties != NULL)
					{
						if (Map_AddOrUpdate(newProperties, GW_SOURCE_PROPERTY, GW_WORKER_MODULE) == MAP_OK)
						{
							if (Map_AddOrUpdate(newProperties, GW_DM_OPERATION, curName) == MAP_OK)
							{
								MESSAGE_BUFFER_CONFIG newMessage;

								newMessage.sourceContent = Message_GetContentHandle(messageHandle);
								newMessage.sourceProperties = newProperties;
								MESSAGE_HANDLE newBusMessage = Message_CreateFromBuffer(&newMessage);
								if (newBusMessage != NULL)
								{
									MessageBus_Publish(moduleData->busHandle, newBusMessage);
								}
								else
								{
									LogError("Failed to create a new message");
								}
							}
						}
						Map_Destroy(newProperties);
					}
					else
					{
						LogError("failed to create a new properties map");
					}
				}
			}
		}
	}

}

/*
 * @brief	Receive a message from the message bus.
 */
static void DMWorker_Receive(MODULE_HANDLE moduleHandle, MESSAGE_HANDLE messageHandle)
{
	if (moduleHandle == NULL || messageHandle == NULL)
	{
		LogError("invalid arguments, module=[%p], message=[%p]", moduleHandle, messageHandle);
	}
	else
	{
		DM_WORKER_DATA * moduleData = (DM_WORKER_DATA*)moduleHandle;

		CONSTMAP_HANDLE properties = Message_GetProperties(messageHandle);
		if (properties != NULL)
		{
			const char * msgId = ConstMap_GetValue(properties, GW_MAC_ADDRESS_PROPERTY);
			const char * msgSource = ConstMap_GetValue(properties, GW_SOURCE_PROPERTY);
			if (!(msgId == NULL || msgSource == NULL))
			{
				if (strcmp(msgId, moduleData->identification) == 0)
				{
					if (strcmp(msgSource, GW_IDMAP_MODULE) == 0)
					{
						const char * msgOperation = ConstMap_GetValue(properties, GW_DM_OPERATION);
						publish_device_message(moduleData, msgOperation, properties);
					}
					else if (strcmp(msgSource, GW_SOURCE_BLE_TELEMETRY) == 0)
					{
						const char * msgCharacteristicUuid = ConstMap_GetValue(properties, GW_CHARACTERISTIC_UUID_PROPERTY);
						if (msgCharacteristicUuid != NULL)
						{
							publish_dm_response(moduleData, msgCharacteristicUuid, messageHandle);
						}
					}
				}
			}
		}
	}
}


/*
 *	Required for all modules:  the public API and the designated implementation functions.
 */
static const MODULE_APIS DMWorker_APIS_all =
{
	DMWorker_Create,
	DMWorker_Destroy,
	DMWorker_Receive
};

#ifdef BUILD_MODULE_TYPE_STATIC
MODULE_EXPORT const MODULE_APIS* MODULE_STATIC_GETAPIS(DM_WORKER_MODULE)(void)
#else
MODULE_EXPORT const MODULE_APIS* Module_GetAPIS(void)
#endif
{
	return &DMWorker_APIS_all;
}
