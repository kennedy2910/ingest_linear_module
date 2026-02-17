#!/usr/bin/env python3
"""Busca metadados de um vídeo do YouTube e retorna sua duração."""

from __future__ import annotations

import argparse
import re
import sys
from datetime import timedelta
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


def formatar_duracao(segundos: int) -> str:
    """Converte segundos para o formato HH:MM:SS."""
    return str(timedelta(seconds=segundos))


def parse_iso8601_duration(valor: str) -> int | None:
    """Converte duração ISO-8601 (ex.: PT1H2M3S) para segundos."""
    match = re.fullmatch(r"PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?", valor)
    if not match:
        return None

    horas = int(match.group(1) or 0)
    minutos = int(match.group(2) or 0)
    segundos = int(match.group(3) or 0)
    return horas * 3600 + minutos * 60 + segundos


def baixar_html(url: str) -> str:
    """Baixa o HTML da página do vídeo."""
    requisicao = Request(
        url,
        headers={
            "User-Agent": (
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            )
        },
    )

    try:
        with urlopen(requisicao, timeout=20) as resposta:
            return resposta.read().decode("utf-8", errors="ignore")
    except HTTPError as exc:
        print(f"Erro HTTP ao acessar a URL: {exc.code}", file=sys.stderr)
        raise SystemExit(1)
    except URLError as exc:
        print(f"Erro de conexão ao acessar a URL: {exc.reason}", file=sys.stderr)
        raise SystemExit(1)


def extrair_titulo(html: str) -> str:
    """Extrai o título do vídeo presente no metadado da página."""
    match = re.search(r'<meta\s+name="title"\s+content="([^"]+)"', html)
    return match.group(1) if match else "N/A"


def extrair_duracao_segundos(html: str) -> int | None:
    """Extrai a duração do vídeo a partir de padrões conhecidos no HTML."""
    length_seconds = re.search(r'"lengthSeconds":"(\d+)"', html)
    if length_seconds:
        return int(length_seconds.group(1))

    duration_iso = re.search(r'itemprop="duration"\s+content="(PT[^"]+)"', html)
    if duration_iso:
        return parse_iso8601_duration(duration_iso.group(1))

    json_ld_iso = re.search(r'"duration":"(PT[^"]+)"', html)
    if json_ld_iso:
        return parse_iso8601_duration(json_ld_iso.group(1))

    return None


def carregar_html_local(caminho: str) -> str:
    """Carrega HTML local para testes/offline."""
    try:
        return Path(caminho).read_text(encoding="utf-8")
    except OSError as exc:
        print(f"Erro ao ler arquivo HTML '{caminho}': {exc}", file=sys.stderr)
        raise SystemExit(1)


def resolver_url(url_posicional: str | None, url_opcao: str | None) -> str | None:
    """Resolve a URL de entrada dando prioridade ao argumento posicional."""
    return url_posicional or url_opcao


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Retorna a duração de um vídeo do YouTube a partir da URL."
    )
    parser.add_argument("url", nargs="?", help="URL do vídeo do YouTube (opcional)")
    parser.add_argument("--url", dest="url_option", help="URL do vídeo do YouTube")
    parser.add_argument(
        "--html-file",
        dest="html_file",
        help="Arquivo HTML local para testes sem rede",
    )
    args = parser.parse_args()

    if args.html_file and (args.url or args.url_option):
        print("Use --html-file ou URL, não ambos ao mesmo tempo.", file=sys.stderr)
        raise SystemExit(2)

    if args.html_file:
        html = carregar_html_local(args.html_file)
    else:
        url = resolver_url(args.url, args.url_option)
        if not url:
            url = input("Cole a URL do vídeo do YouTube: ").strip()
        if not url:
            print("URL inválida: informe uma URL do YouTube.", file=sys.stderr)
            raise SystemExit(2)
        html = baixar_html(url)

    duracao_segundos = extrair_duracao_segundos(html)

    if duracao_segundos is None:
        print("Não foi possível obter a duração do vídeo nessa fonte.", file=sys.stderr)
        raise SystemExit(2)

    print(f"Título: {extrair_titulo(html)}")
    print(f"Duração (segundos): {duracao_segundos}")
    print(f"Duração (HH:MM:SS): {formatar_duracao(duracao_segundos)}")


if __name__ == "__main__":
    main()
