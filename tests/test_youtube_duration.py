import unittest

from youtube_duration import (
    extrair_duracao_segundos,
    formatar_duracao,
    parse_iso8601_duration,
    resolver_url,
)


class YoutubeDurationTests(unittest.TestCase):
    def test_extrai_length_seconds(self):
        html = '<script>"lengthSeconds":"3661"</script>'
        self.assertEqual(extrair_duracao_segundos(html), 3661)

    def test_fallback_iso8601_itemprop(self):
        html = '<meta itemprop="duration" content="PT1H2M3S" />'
        self.assertEqual(extrair_duracao_segundos(html), 3723)

    def test_parse_iso8601(self):
        self.assertEqual(parse_iso8601_duration("PT4M7S"), 247)
        self.assertIsNone(parse_iso8601_duration("INVALID"))

    def test_formatar_duracao(self):
        self.assertEqual(formatar_duracao(3661), "1:01:01")

    def test_resolver_url_prioriza_posicional(self):
        self.assertEqual(resolver_url("https://a", "https://b"), "https://a")
        self.assertEqual(resolver_url(None, "https://b"), "https://b")


if __name__ == "__main__":
    unittest.main()
