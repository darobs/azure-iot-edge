// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

#include <stdlib.h>
#ifdef _CRTDBG_MAP_ALLOC
#include <crtdbg.h>
#endif
#include "azure_c_shared_utility/gballoc.h"

#include "dmhub_hl.h"
#include "dmhub.h"
#include "azure_c_shared_utility/iot_logging.h"
#include "azure_c_shared_utility/vector.h"
#include "parson.h"

#define SUFFIX "IoTHubSuffix"
#define HUBNAME "IoTHubName"

static MODULE_HANDLE dmhub_HL_Create(MESSAGE_BUS_HANDLE busHandle, const void* configuration)
{
    MODULE_HANDLE *result;
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
            JSON_Object* obj = json_value_get_object(json);
            if (obj == NULL)
            {
                LogError("Expected a JSON Object in configuration");
                result = NULL;
            }
            else
            {
                const char * IoTHubName;
                const char * IoTHubSuffix;
                if ((IoTHubName = json_object_get_string(obj, HUBNAME)) == NULL)
                {
                    LogError("Did not find expected %s configuration", HUBNAME);
                    result = NULL;
                }
                else if ((IoTHubSuffix = json_object_get_string(obj, SUFFIX)) == NULL)
                {
                    LogError("Did not find expected %s configuration", SUFFIX);
                    result = NULL;
                }
                else
				{
                    DMHUB_CONFIG llConfiguration;
                    llConfiguration.IoTHubName = IoTHubName;
                    llConfiguration.IoTHubSuffix = IoTHubSuffix;
                    result = MODULE_STATIC_GETAPIS(DMHUB_MODULE)()->Module_Create(busHandle, &llConfiguration);
                }
            }
            json_value_free(json);
        }
    }
    return result;
}

static void dmhub_HL_Destroy(MODULE_HANDLE moduleHandle)
{
    MODULE_STATIC_GETAPIS(DMHUB_MODULE)()->Module_Destroy(moduleHandle);
}


static void dmhub_HL_Receive(MODULE_HANDLE moduleHandle, MESSAGE_HANDLE messageHandle)
{
    MODULE_STATIC_GETAPIS(DMHUB_MODULE)()->Module_Receive(moduleHandle, messageHandle);
}

static const MODULE_APIS dmhub_HL_GetAPIS_Impl =
{
    dmhub_HL_Create,
    dmhub_HL_Destroy,
    dmhub_HL_Receive
};


#ifdef BUILD_MODULE_TYPE_STATIC
MODULE_EXPORT const MODULE_APIS* MODULE_STATIC_GETAPIS(DMHUB_MODULE_HL)(void)
#else
MODULE_EXPORT const MODULE_APIS* Module_GetAPIS(void)
#endif
{
    return &dmhub_HL_GetAPIS_Impl;
}