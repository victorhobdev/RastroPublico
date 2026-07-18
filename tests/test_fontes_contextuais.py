from rastro_publico.coleta.fontes_contextuais import definir_fontes


def test_define_fontes_oficiais_versionadas() -> None:
    fontes = definir_fontes(
        data_referencia="20260718",
        periodo_inicial_ipca="202506",
        periodo_final_ipca="202606",
    )

    assert set(fontes) == {"municipios_ibge", "ipca_indice", "ceis", "cnep"}
    assert fontes["municipios_ibge"]["sistema_origem"] == "ibge"
    assert "/t/1737/n1/all/v/2266/p/202506-202606" in fontes["ipca_indice"][
        "url"
    ]
    assert fontes["ceis"]["url"].endswith("/ceis/20260718")
    assert fontes["cnep"]["url"].endswith("/cnep/20260718")
