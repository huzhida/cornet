#ifndef CORNET_ATASK_H
#define CORNET_ATASK_H

#include "task.h"

namespace cornet {

struct atask_t : task_t {
  void (*fn) (atask_t*){};
};

}


#endif //CORNET_ATASK_H
