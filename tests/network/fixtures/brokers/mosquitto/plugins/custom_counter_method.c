// Test-only enhanced authentication method CUSTOM-COUNTER-METHOD: the client sends "1", the
// broker challenges with "2", and the client must answer "3". Re-authentication repeats it.

#include <stdlib.h>
#include <string.h>

#include <mosquitto.h>
#include <mosquitto_broker.h>
#include <mosquitto_plugin.h>

#define CUSTOM_COUNTER_METHOD "CUSTOM-COUNTER-METHOD"

static mosquitto_plugin_id_t *plugin_id;

static int data_is(const struct mosquitto_evt_extended_auth *auth, char expected)
{
    return auth->data_in_len == 1 && ((const char *)auth->data_in)[0] == expected;
}

static int on_auth_start(int event, void *event_data, void *userdata)
{
    struct mosquitto_evt_extended_auth *auth = event_data;
    (void)event;
    (void)userdata;

    if (strcmp(auth->auth_method, CUSTOM_COUNTER_METHOD) != 0) {
        return MOSQ_ERR_PLUGIN_DEFER;
    }
    if (!data_is(auth, '1')) {
        return MOSQ_ERR_AUTH;
    }

    // Mosquitto releases the challenge with free(), not mosquitto_free().
    auth->data_out = malloc(1);
    if (auth->data_out == NULL) {
        return MOSQ_ERR_NOMEM;
    }
    ((char *)auth->data_out)[0] = '2';
    auth->data_out_len = 1;
    return MOSQ_ERR_AUTH_CONTINUE;
}

static int on_auth_continue(int event, void *event_data, void *userdata)
{
    struct mosquitto_evt_extended_auth *auth = event_data;
    (void)event;
    (void)userdata;

    // Mosquitto 2.0 leaves auth_method unset here; this is the only plugin that starts exchanges.
    return data_is(auth, '3') ? MOSQ_ERR_SUCCESS : MOSQ_ERR_AUTH;
}

int mosquitto_plugin_version(int supported_version_count, const int *supported_versions)
{
    for (int i = 0; i < supported_version_count; i++) {
        if (supported_versions[i] == 5) {
            return 5;
        }
    }
    return -1;
}

int mosquitto_plugin_init(
    mosquitto_plugin_id_t *identifier,
    void **userdata,
    struct mosquitto_opt *options,
    int option_count)
{
    (void)userdata;
    (void)options;
    (void)option_count;

    plugin_id = identifier;
    int rc = mosquitto_callback_register(
        plugin_id, MOSQ_EVT_EXT_AUTH_START, on_auth_start, NULL, NULL);
    if (rc != MOSQ_ERR_SUCCESS) {
        return rc;
    }
    return mosquitto_callback_register(
        plugin_id, MOSQ_EVT_EXT_AUTH_CONTINUE, on_auth_continue, NULL, NULL);
}

int mosquitto_plugin_cleanup(void *userdata, struct mosquitto_opt *options, int option_count)
{
    (void)userdata;
    (void)options;
    (void)option_count;

    mosquitto_callback_unregister(plugin_id, MOSQ_EVT_EXT_AUTH_START, on_auth_start, NULL);
    mosquitto_callback_unregister(plugin_id, MOSQ_EVT_EXT_AUTH_CONTINUE, on_auth_continue, NULL);
    return MOSQ_ERR_SUCCESS;
}
