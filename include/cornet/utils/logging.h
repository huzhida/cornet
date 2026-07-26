#ifndef CORNET_LOGGING_H
#define CORNET_LOGGING_H

#include <spdlog/spdlog.h>

#include "cornet/utils/config.h"

namespace cornet::logging {
/**
 * @brief initialize spdlog logging from config.
 * Must be called after config_t::load().
 */
void init(const config_t& config);

}

#endif //CORNET_LOGGING_H
