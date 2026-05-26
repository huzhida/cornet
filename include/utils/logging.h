#ifndef CORNET_LOGGING_H
#define CORNET_LOGGING_H

#include "config.h"
#include <spdlog/spdlog.h>

namespace cornet::logging {

/**
 * @brief initialize spdlog logging from config.
 * Must be called after config_t::load().
 */
void init();

}

#endif //CORNET_LOGGING_H
