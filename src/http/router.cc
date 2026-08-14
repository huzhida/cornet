#include "cornet/http/router.h"

#include <algorithm>
#include <unordered_map>

#include <spdlog/spdlog.h>

namespace cornet::http {

namespace {

/**
 * @brief split a path into segments, skipping empty ones so that "/a//b" and
 * "/a/b" route the same.
 */
void split_path(std::string_view path, std::vector<std::string_view>& out) {
  out.clear();
  size_t i = 0;
  while (i < path.size()) {
    while (i < path.size() && path[i] == '/') ++i;
    if (i >= path.size()) break;
    size_t j = path.find('/', i);
    if (j == std::string_view::npos) j = path.size();
    out.push_back(path.substr(i, j - i));
    i = j;
  }
}

/**
 * @brief method index for the per-node route table.
 * A small dense array beats a map: at most nine methods can match a node, and the
 * lookup is on the hot path.
 */
constexpr uint32_t kMethodSlots = 10;

uint32_t method_slot(method_t m) {
  switch (m) {
    case method_t::Get: return 0;
    case method_t::Head: return 1;
    case method_t::Post: return 2;
    case method_t::Put: return 3;
    case method_t::Delete: return 4;
    case method_t::Patch: return 5;
    case method_t::Options: return 6;
    case method_t::Connect: return 7;
    case method_t::Trace: return 8;
    default: return 9;
  }
}

} // namespace

/**
 * @brief router internals.
 *
 * Exact paths land in a hash table keyed by (method, path) — one probe, no node
 * walking, which is what the overwhelming majority of real routes are.
 * Parameterised paths go into a trie walked segment by segment; parameter names
 * live in the route so a match only records value positions.
 */
struct router_t::impl_t {
  struct node_t {
    // literal children, keyed by segment text
    std::unordered_map<std::string, std::unique_ptr<node_t>> literal;
    // ":name" child, if any
    std::unique_ptr<node_t> param;
    // "*name" child, which swallows the rest of the path
    std::unique_ptr<node_t> wildcard;
    // routes terminating at this node, by method
    std::unique_ptr<route_t> routes[kMethodSlots]{};

    CORNET_NODISCARD bool has_any_route() const {
      for (const auto& r : routes) {
        if (r) return true;
      }
      return false;
    }
  };

  // exact-match table: key is method slot plus the normalised path
  std::unordered_map<std::string, std::unique_ptr<route_t>> exact;
  node_t root;

  static std::string exact_key(method_t m, std::string_view path) {
    std::string key;
    key.reserve(path.size() + 1);
    key.push_back(char('0' + method_slot(m)));
    key.append(path);
    return key;
  }

  /**
   * @brief normalise a path for keying: leading slash, no trailing slash,
   * no empty segments.
   */
  static std::string normalise(std::string_view path) {
    std::vector<std::string_view> segs;
    split_path(path, segs);
    std::string out;
    out.reserve(path.size() + 1);
    for (auto s : segs) {
      out.push_back('/');
      out.append(s);
    }
    if (out.empty()) out.push_back('/');
    return out;
  }
};

router_t::router_t() : impl_(std::make_unique<impl_t>()) {}
router_t::~router_t() = default;
router_t::router_t(router_t&&) noexcept = default;
router_t& router_t::operator=(router_t&&) noexcept = default;

route_t& router_t::add(method_t m, std::string_view path, route_t route) {
  std::vector<std::string_view> segs;
  split_path(path, segs);

  bool dynamic = std::any_of(segs.begin(), segs.end(), [](std::string_view s) {
    return !s.empty() && (s[0] == ':' || s[0] == '*');
  });

  ++count_;

  if (!dynamic) {
    auto key = impl_t::exact_key(m, impl_t::normalise(path));
    auto slot = std::make_unique<route_t>(std::move(route));
    auto* raw = slot.get();
    auto [it, inserted] = impl_->exact.emplace(std::move(key), std::move(slot));
    if (!inserted) {
      SPDLOG_WARN("http: route {} {} registered twice, replacing", method_name(m), path);
      it->second = std::make_unique<route_t>(std::move(*raw));
      return *it->second;
    }
    return *it->second;
  }

  auto* node = &impl_->root;
  for (auto seg : segs) {
    if (seg[0] == ':') {
      route.param_names.emplace_back(seg.substr(1));
      if (!node->param) node->param = std::make_unique<impl_t::node_t>();
      node = node->param.get();
    } else if (seg[0] == '*') {
      route.param_names.emplace_back(seg.substr(1));
      if (!node->wildcard) node->wildcard = std::make_unique<impl_t::node_t>();
      node = node->wildcard.get();
      break;  // a wildcard consumes everything after it
    } else {
      auto key = std::string(seg);
      auto it = node->literal.find(key);
      if (it == node->literal.end()) {
        it = node->literal.emplace(std::move(key), std::make_unique<impl_t::node_t>()).first;
      }
      node = it->second.get();
    }
  }

  auto slot = method_slot(m);
  if (node->routes[slot]) {
    SPDLOG_WARN("http: route {} {} registered twice, replacing", method_name(m), path);
  }
  node->routes[slot] = std::make_unique<route_t>(std::move(route));
  return *node->routes[slot];
}

match_t router_t::match(method_t m, std::string_view path, param_slots_t& out) const {
  out.clear();

  // Exact table first: one hash probe answers most requests.
  {
    auto key = impl_t::exact_key(m, impl_t::normalise(path));
    auto it = impl_->exact.find(key);
    if (it != impl_->exact.end()) {
      return match_t{it->second.get(), false};
    }
  }

  std::vector<std::string_view> segs;
  split_path(path, segs);

  // Walk the trie. Literal children win over ':name', which wins over '*rest',
  // so a specific route is never shadowed by a general one.
  struct frame_t {
    const impl_t::node_t* node;
    uint32_t seg;
    uint32_t params;
  };

  std::string_view captured[param_slots_t::kMax];
  uint32_t captured_n = 0;

  const impl_t::node_t* node = &impl_->root;
  uint32_t i = 0;
  bool wildcard_hit = false;

  // Small explicit backtracking stack: only ':' and '*' create alternatives, and
  // real route tables are shallow.
  std::vector<frame_t> stack;

  auto try_descend = [&]() -> bool {
    while (i < segs.size()) {
      auto seg = segs[i];
      auto lit = node->literal.find(std::string(seg));
      if (lit != node->literal.end()) {
        // remember the alternatives in case this branch dead-ends
        if (node->param || node->wildcard) {
          stack.push_back(frame_t{node, i, captured_n});
        }
        node = lit->second.get();
        ++i;
        continue;
      }
      if (node->param) {
        if (captured_n < param_slots_t::kMax) captured[captured_n++] = seg;
        if (node->wildcard) stack.push_back(frame_t{node, i, captured_n - 1});
        node = node->param.get();
        ++i;
        continue;
      }
      if (node->wildcard) {
        auto rest_start = size_t(segs[i].data() - path.data());
        if (captured_n < param_slots_t::kMax) {
          captured[captured_n++] = path.substr(rest_start);
        }
        node = node->wildcard.get();
        i = uint32_t(segs.size());
        wildcard_hit = true;
        continue;
      }
      return false;
    }
    return true;
  };

  bool matched = try_descend();
  while (!matched && !stack.empty()) {
    auto frame = stack.back();
    stack.pop_back();
    node = frame.node;
    i = frame.seg;
    captured_n = frame.params;
    // retry this node preferring the parameterised alternatives
    if (node->param) {
      if (captured_n < param_slots_t::kMax) captured[captured_n++] = segs[i];
      node = node->param.get();
      ++i;
    } else if (node->wildcard) {
      auto rest_start = size_t(segs[i].data() - path.data());
      if (captured_n < param_slots_t::kMax) captured[captured_n++] = path.substr(rest_start);
      node = node->wildcard.get();
      i = uint32_t(segs.size());
      wildcard_hit = true;
    } else {
      continue;
    }
    matched = try_descend();
  }
  (void)wildcard_hit;

  if (!matched) return {};

  auto slot = method_slot(m);
  const route_t* route = node->routes[slot].get();
  if (!route) {
    // The path exists but not for this method: worth distinguishing so the caller
    // can answer 405 instead of 404.
    return match_t{nullptr, node->has_any_route()};
  }

  // Names come from the route (fixed at registration), values from the walk, so
  // the match itself records only positions.
  for (uint32_t k = 0; k < captured_n && k < route->param_names.size(); ++k) {
    out.add(route->param_names[k], captured[k]);
  }
  return match_t{route, false};
}

} // namespace cornet::http
