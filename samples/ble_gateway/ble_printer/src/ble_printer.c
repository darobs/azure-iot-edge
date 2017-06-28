// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <math.h>

#include <glib.h>

#ifdef _CRTDBG_MAP_ALLOC
#include <crtdbg.h>
#endif

#include "azure_c_shared_utility/gballoc.h"
#include "azure_c_shared_utility/xlogging.h"
#include "azure_c_shared_utility/constmap.h"
#include "azure_c_shared_utility/constbuffer.h"
#include "azure_c_shared_utility/strings.h"

#include "module.h"
#include "message.h"
#include "broker.h"
#include "ble.h"
#include "ble_printer.h"
#include "messageproperties.h"

typedef STRING_HANDLE(*MESSAGE_PRINTER)(const char* name, const char* timestamp, const CONSTBUFFER* buffer);

static STRING_HANDLE print_string(const char* name, const char* timestamp, const CONSTBUFFER* buffer);
static STRING_HANDLE print_temperature(const char* name, const char* timestamp, const CONSTBUFFER* buffer);
static STRING_HANDLE print_default(const char* name, const char* timestamp, const CONSTBUFFER* buffer);

typedef struct BLE_JSON_CONVERT_TAG
{
    BROKER_HANDLE broker;
} BLE_JSON_CONVERT;

typedef struct DIPATCH_ENTRY_TAG
{
    const char* name;
    const char* characteristic_uuid;
    MESSAGE_PRINTER message_printer;
}DIPATCH_ENTRY;

// setup the message printers
DIPATCH_ENTRY g_dispatch_entries[] =
{
    {
        "Model Number",
        "00002A24-0000-1000-8000-00805F9B34FB",
        print_string
    },
    {
        "Serial Number",
        "00002A25-0000-1000-8000-00805F9B34FB",
        print_string
    },
    {
        "Serial Number",
        "00002A25-0000-1000-8000-00805F9B34FB",
        print_string
    },
    {
        "Firmware Revision Number",
        "00002A26-0000-1000-8000-00805F9B34FB",
        print_string
    },
    {
        "Hardware Revision Number",
        "00002A27-0000-1000-8000-00805F9B34FB",
        print_string
    },
    {
        "Software Revision Number",
        "00002A28-0000-1000-8000-00805F9B34FB",
        print_string
    },
    {
        "Manufacturer",
        "00002A29-0000-1000-8000-00805F9B34FB",
        print_string
    },
    {
        "Temperature",
        "F000AA01-0451-4000-B000-000000000000",
        print_temperature
    }
};

size_t g_dispatch_entries_length = sizeof(g_dispatch_entries) / sizeof(g_dispatch_entries[0]);

MODULE_HANDLE BLEPrinter_Create(BROKER_HANDLE broker, const void* configuration)
{
    BLE_JSON_CONVERT* ble_json_convert = (BLE_JSON_CONVERT*)malloc(sizeof(BLE_JSON_CONVERT));
    if (ble_json_convert != NULL)
    {
        ble_json_convert->broker = broker;
    }
    (void)configuration;
    return (MODULE_HANDLE)ble_json_convert;
}

void* BLEPrinter_ParseConfigurationFromJson(const char* configuration)
{
    (void)configuration;
    return NULL;
}

void BLEPrinter_FreeConfiguration(void * configuration)
{
    (void)configuration;
    return;
}

void BLEPrinter_Receive(MODULE_HANDLE module, MESSAGE_HANDLE message)
{
    BLE_JSON_CONVERT* ble_json_convert = (BLE_JSON_CONVERT*)module;
    if (message != NULL)
    {
        CONSTMAP_HANDLE props = Message_GetProperties(message);
        if (props != NULL)
        {
            const char* source = ConstMap_GetValue(props, GW_SOURCE_PROPERTY);
            if (source != NULL && strcmp(source, GW_SOURCE_BLE_TELEMETRY) == 0)
            {
                //const char* ble_controller_id = ConstMap_GetValue(props, GW_BLE_CONTROLLER_INDEX_PROPERTY);
                //const char* mac_address_str = ConstMap_GetValue(props, GW_MAC_ADDRESS_PROPERTY);
                STRING_HANDLE message_body;
                const char* timestamp = ConstMap_GetValue(props, GW_TIMESTAMP_PROPERTY);
                const char* characteristic_uuid = ConstMap_GetValue(props, GW_CHARACTERISTIC_UUID_PROPERTY);
                const CONSTBUFFER* buffer = Message_GetContent(message);
                if (buffer != NULL && characteristic_uuid != NULL)
                {
                    // dispatch the message based on the characteristic uuid
                    size_t i;
                    for (i = 0; i < g_dispatch_entries_length; i++)
                    {
                        if (g_ascii_strcasecmp(
                                characteristic_uuid,
                                g_dispatch_entries[i].characteristic_uuid
                            ) == 0)
                        {
                            message_body = g_dispatch_entries[i].message_printer(
                                g_dispatch_entries[i].name,
                                timestamp,
                                buffer
                            );
                            break;
                        }
                    }

                    if (i == g_dispatch_entries_length)
                    {
                        // dispatch to default printer
                        message_body = print_default(characteristic_uuid, timestamp, buffer);
                    }

                    if (message_body != NULL)
                    {
                        MESSAGE_CONFIG new_message_config =
                        {
                            STRING_length(message_body) + 1,
                            STRING_c_str(message_body),
                            props
                        };
                        MESSAGE_HANDLE new_message = Message_Create(&new_message_config);
                        if (new_message != NULL)
                        {
                            Broker_Publish(ble_json_convert->broker, module, new_message);
                        }

                        STRING_delete(message_body);
                    }

                }
                else
                {
                    LogError("Message is invalid. Nothing to print.");
                }
            }
        }
        else
        {
            LogError("Message_GetProperties for the message returned NULL");
        }
    }
    else
    {
        LogError("message is NULL");
    }
}

void BLEPrinter_Destroy(MODULE_HANDLE module)
{
    free(module);
}

static const MODULE_API_1 Module_GetApi_Impl =
{
    {MODULE_API_VERSION_1},

    BLEPrinter_ParseConfigurationFromJson,
    BLEPrinter_FreeConfiguration,
    BLEPrinter_Create,
    BLEPrinter_Destroy,
    BLEPrinter_Receive,
    NULL

};

MODULE_EXPORT const MODULE_API* Module_GetApi(MODULE_API_VERSION gateway_api_version)
{
    (void)gateway_api_version;
    return (const MODULE_API*)&Module_GetApi_Impl;
}

static STRING_HANDLE print_string(const char* name, const char* timestamp, const CONSTBUFFER* buffer)
{
    STRING_HANDLE result = STRING_construct_sprintf("{\"timestamp\":\"%s\",\"%s\":\"%.*s\"}", 
        timestamp, name, (int)buffer->size, buffer->buffer);
    return result;
}

/**
 * Code taken from:
 *    http://processors.wiki.ti.com/index.php/CC2650_SensorTag_User's_Guide#Data
 */
static void sensortag_temp_convert(
    uint16_t rawAmbTemp,
    uint16_t rawObjTemp,
    float *tAmb,
    float *tObj
)
{
    const float SCALE_LSB = 0.03125;
    float t;
    int it;

    it = (int)((rawObjTemp) >> 2);
    t = ((float)(it)) * SCALE_LSB;
    *tObj = t;

    it = (int)((rawAmbTemp) >> 2);
    t = (float)it;
    *tAmb = t * SCALE_LSB;
}

static STRING_HANDLE print_temperature(const char* name, const char* timestamp, const CONSTBUFFER* buffer)
{
    STRING_HANDLE result = NULL;
    if (buffer->size == 4)
    {
        uint16_t* temps = (uint16_t *)buffer->buffer;
        float ambient, object;
        sensortag_temp_convert(temps[0], temps[1], &ambient, &object);
        result = STRING_construct_sprintf("{\"timestamp\":\"%s\",\"%s.ambient\":\"%f\",\"%s.object\":\"%f\"}",
            timestamp,
            name,
            ambient,
            name,
            object
        );
    }
    return result;
}

static void print_default(const char* name, const char* timestamp, const CONSTBUFFER* buffer)
{
    STRING_HANDLE result = STRING_construct_sprintf("{\"timestamp\":\"%s\",\"%s\":\"",
        timestamp, name);
    char hex_char[4]
    for (size_t i = 0; i < buffer->size; ++i)
    {
        sprintf(hex_char, "%02X ", buffer->buffer[i]);
        STRING_concat(result, hex_char);
    }
    STRING_concat(result, "\"}");
    return result;
}
