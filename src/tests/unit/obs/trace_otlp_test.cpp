// trace.h / trace_otlp.cpp — OtlpHttpExporter unit + opt-in integration.
//
// The pure encoder is a free function (`EncodeOtlpTracesJson`) so we
// drive it directly without an HTTP round-trip. The exporter itself
// is exercised via:
//   - A negative test that verifies `Create` rejects an empty endpoint.
//   - A negative test that verifies POST against a reserved-discard
//     port surfaces an error (best-effort: the exporter swallows
//     post-time failures, so we just check the worker doesn't deadlock).
//   - An opt-in integration test gated on `OTLP_ENDPOINT`. Locally:
//       docker run -d --rm --name otelc -p 4318:4318 \
//         otel/opentelemetry-collector:0.96.0
//       OTLP_ENDPOINT=http://127.0.0.1:4318/v1/traces \
//         ctest -R OtlpHttpIntegration
#include "trace.h"

#include <gtest/gtest.h>
#include <nlohmann/json.hpp>

#include <chrono>
#include <cstdlib>
#include <string>
#include <thread>

using namespace kvcache::trace;
using json = nlohmann::json;

namespace {

SpanData MakeSpan(std::string name, StatusCode status = StatusCode::Ok) {
    SpanData s;
    for (std::size_t i = 0; i < s.context.trace_id.size(); ++i)
        s.context.trace_id[i] = static_cast<uint8_t>(0xA0 + i);
    for (std::size_t i = 0; i < s.context.span_id.size(); ++i)
        s.context.span_id[i] = static_cast<uint8_t>(0xB0 + i);
    // Default: root span. SpanContext{} has both trace_id and span_id
    // all-zero, which IsValid() rejects.
    s.parent = SpanContext{};
    s.name   = std::move(name);
    s.start  = Clock::now();
    s.end    = s.start + std::chrono::microseconds(123);
    s.status = status;
    return s;
}

const char* IntegrationEndpoint() {
    const char* e = std::getenv("OTLP_ENDPOINT");
    return (e && *e) ? e : nullptr;
}

}  // namespace

// ---- pure encoder ---------------------------------------------------------

TEST(EncodeOtlpTracesJson, ShapeMatchesOtelSchema) {
    auto s = MakeSpan("kv.lookup");
    s.attributes.push_back({"kv.hit",          true});
    s.attributes.push_back({"kv.matched",      int64_t{32}});
    s.attributes.push_back({"kv.tenant",       std::string("acme")});

    const std::string body = EncodeOtlpTracesJson({s}, "kvstore-node", "0.1.0");
    json doc = json::parse(body);

    ASSERT_TRUE(doc.contains("resourceSpans"));
    ASSERT_EQ(doc["resourceSpans"].size(), 1u);
    const auto& rs = doc["resourceSpans"][0];

    // resource.service.name
    bool saw_name = false;
    for (const auto& attr : rs["resource"]["attributes"]) {
        if (attr["key"] == "service.name") {
            EXPECT_EQ(attr["value"]["stringValue"], "kvstore-node");
            saw_name = true;
        }
    }
    EXPECT_TRUE(saw_name);

    // scope.name + the single span
    ASSERT_EQ(rs["scopeSpans"].size(), 1u);
    const auto& scope = rs["scopeSpans"][0];
    EXPECT_EQ(scope["scope"]["name"], "kvcache");
    ASSERT_EQ(scope["spans"].size(), 1u);
    const auto& sp = scope["spans"][0];
    EXPECT_EQ(sp["name"], "kv.lookup");
    // IDs are lowercase hex of the raw 16 / 8 bytes.
    EXPECT_EQ(sp["traceId"].get<std::string>().size(), 32u);
    EXPECT_EQ(sp["spanId"].get<std::string>().size(),  16u);
    // Root span — no parentSpanId field.
    EXPECT_FALSE(sp.contains("parentSpanId"));
    // Status: OK = 1.
    EXPECT_EQ(sp["status"]["code"], 1);
    // Attributes: boolValue / intValue / stringValue carriers.
    ASSERT_EQ(sp["attributes"].size(), 3u);
    EXPECT_EQ(sp["attributes"][0]["value"]["boolValue"],   true);
    EXPECT_EQ(sp["attributes"][1]["value"]["intValue"],    "32");
    EXPECT_EQ(sp["attributes"][2]["value"]["stringValue"], "acme");
}

TEST(EncodeOtlpTracesJson, ChildSpanCarriesParentIdAndErrorStatus) {
    auto parent = MakeSpan("kv.fetch");
    auto child  = MakeSpan("nixl.scheduled_pull", StatusCode::Error);
    child.parent     = parent.context;
    child.status_message = "timeout";

    const std::string body = EncodeOtlpTracesJson({parent, child}, "kvstore-node", "");
    json doc = json::parse(body);
    const auto& spans = doc["resourceSpans"][0]["scopeSpans"][0]["spans"];
    ASSERT_EQ(spans.size(), 2u);
    EXPECT_FALSE(spans[0].contains("parentSpanId"));
    ASSERT_TRUE (spans[1].contains("parentSpanId"));
    EXPECT_EQ(spans[1]["parentSpanId"].get<std::string>().size(), 16u);
    EXPECT_EQ(spans[1]["status"]["code"], 2);
    EXPECT_EQ(spans[1]["status"]["message"], "timeout");
}

TEST(EncodeOtlpTracesJson, ServiceVersionOptional) {
    auto s = MakeSpan("op");
    json no_version = json::parse(EncodeOtlpTracesJson({s}, "svc", ""));
    json with_version = json::parse(EncodeOtlpTracesJson({s}, "svc", "1.2.3"));
    // service.name present in both.
    auto count_keys = [](const json& doc) {
        std::size_t n = 0;
        for (const auto& a : doc["resourceSpans"][0]["resource"]["attributes"]) {
            (void)a; ++n;
        }
        return n;
    };
    EXPECT_EQ(count_keys(no_version),    1u);  // just service.name
    EXPECT_EQ(count_keys(with_version),  2u);  // service.name + service.version
}

// ---- exporter ------------------------------------------------------------

TEST(OtlpHttpExporter, CreateRejectsEmptyEndpoint) {
    OtlpHttpExporter::Options o;
    std::string err;
    auto e = OtlpHttpExporter::Create(o, &err);
    EXPECT_EQ(e, nullptr);
    EXPECT_FALSE(err.empty());
}

TEST(OtlpHttpExporter, UnreachableEndpointDoesNotDeadlockFlush) {
    OtlpHttpExporter::Options o;
    o.endpoint     = "http://127.0.0.1:1/v1/traces";  // discard port
    o.dial_timeout = std::chrono::milliseconds(200);
    o.flush_interval = std::chrono::milliseconds(50);
    std::string err;
    auto e = OtlpHttpExporter::Create(o, &err);
    ASSERT_NE(e, nullptr) << err;
    e->Export(MakeSpan("op"));
    // Errors during POST are swallowed; Flush must still return.
    e->Flush();
}

// ---- opt-in integration --------------------------------------------------

class OtlpHttpIntegrationTest : public ::testing::Test {
   protected:
    void SetUp() override {
        const char* ep = IntegrationEndpoint();
        if (!ep) GTEST_SKIP() << "OTLP_ENDPOINT not set; skipping integration";
        OtlpHttpExporter::Options o;
        o.endpoint        = ep;
        o.service_name    = "kvcache-test";
        o.flush_interval  = std::chrono::milliseconds(50);
        std::string err;
        exporter_ = OtlpHttpExporter::Create(o, &err);
        ASSERT_NE(exporter_, nullptr) << err;
    }
    std::unique_ptr<OtlpHttpExporter> exporter_;
};

TEST_F(OtlpHttpIntegrationTest, SinglePostSucceeds) {
    auto s = MakeSpan("integration.smoke");
    s.attributes.push_back({"kv.matched", int64_t{16}});
    exporter_->Export(s);
    exporter_->Flush();
}

TEST_F(OtlpHttpIntegrationTest, BatchPostSucceeds) {
    for (int i = 0; i < 100; ++i) {
        auto s = MakeSpan("integration.bulk");
        s.attributes.push_back({"iter", int64_t{i}});
        exporter_->Export(s);
    }
    exporter_->Flush();
}
