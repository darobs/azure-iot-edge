// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

#ifndef IOTHUBHTTP_H
#define IOTHUBHTTP_H

#include "module.h"
#include "azure_c_shared_utility/vector.h"

#ifdef __cplusplus
extern "C"
{
#endif

typedef struct DMHUB_CONFIG_TAG
{
	const char* IoTHubName;
	const char* IoTHubSuffix;
}DMHUB_CONFIG; /*this needs to be passed to the Module_Create function*/

MODULE_EXPORT const MODULE_APIS* MODULE_STATIC_GETAPIS(DMHUB_MODULE)(void);

#ifdef __cplusplus
}
#endif

#endif /*IOTHUBHTTP_H*/
