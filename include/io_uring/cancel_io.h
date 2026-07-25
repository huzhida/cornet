#ifndef CORNET_CANCEL_IO_H
#define CORNET_CANCEL_IO_H

namespace cornet {

struct cancel_node;
struct context_t;

/**
 * @brief io_uring-specific cancellation logic, isolated from canceler_t
 * so that the io_uring cancel API is only used in one place.
 *
 * This allows future replacement with a different implementation for
 * non-io_uring backends (e.g., epoll).
 */
class canceler_io_t {
 public:
  // Called when canceler is linked to an io_uring operation
  void link_node(cancel_node* node);
  void unlink_node(cancel_node* node);

  // Called when cancel() is invoked -- issues io_uring cancel operations
  void cancel_active_tasks();

  // Accessors needed by canceler_t
  cancel_node* active_head_ = nullptr;
  context_t* ctx_ = nullptr;

  // Default constructor
  canceler_io_t() = default;

  // Copy constructor — ctx_ is NOT copied from parent (each canceler should have its own context)
  canceler_io_t(const canceler_io_t& other)
    : active_head_(nullptr), ctx_(nullptr) {}

  void set_ctx(context_t* ctx) { ctx_ = ctx; }

 private:
};

} // namespace cornet

#endif // CORNET_CANCEL_IO_H
