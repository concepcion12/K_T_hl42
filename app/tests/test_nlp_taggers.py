from nlp.taggers import detect_disciplines, detect_themes


def test_detect_music_and_visual_disciplines():
    text = "Guam-based photographer and music producer releasing a new EP."
    tags = detect_disciplines(text)
    assert "visual" in tags
    assert "music" in tags


def test_detect_craft_and_culinary_disciplines():
    text = "Handmade jewelry maker and private chef hosting a pop-up dinner in Hagatna."
    tags = detect_disciplines(text)
    assert "craft" in tags
    assert "culinary" in tags


def test_detect_activist_discipline_and_themes():
    text = (
        "Community organizer leading mutual aid projects focused on food security, "
        "climate justice, and cultural preservation across Guam."
    )
    discipline_tags = detect_disciplines(text)
    theme_tags = detect_themes(text)

    assert "activist" in discipline_tags
    assert "community_wellness" in theme_tags
    assert "food_security" in theme_tags
    assert "sustainability" in theme_tags
    assert "cultural_preservation" in theme_tags


def test_detect_performing_discipline():
    text = "Dancer and choreographer performing a new theatre piece at Artspace Guahan."
    tags = detect_disciplines(text)
    assert "performing" in tags
