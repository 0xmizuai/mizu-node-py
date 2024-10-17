from mizu_node.worker.classifier import classify
from mizu_node.worker.embeddings.domain_embeddings import V1_EMBEDDING


def test_classify():
    # Given
    text = "Coding"

    # When
    res = classify(text, V1_EMBEDDING, 2)

    # Then
    assert res == ["Coding", "Codes"]


def test_classify_long_text():
    # Given
    text = "Laura Bell Bundy is set to release her brand new album, Another Piece of Me, on June 9. The album features 15 brand new songs with more that half co-written and co-produced by the talented country artist. Bundy digs deep and gets personal with this new collection of music.\nLaura Bell Bundy shared details with us about her new music as well as her involvement on the project, inspirations and so much more. We’re excited to see (and hear) Laura Bell Bundy on this new journey and anticipate a lot of great things heading her way.\nCountryMusicRocks (CMR): You’re gearing up to release your new album, Another Piece of Me. Give us a little background and tell us about this new collection of music.\nCMR: It looks like you’ve been pretty hands on with this new album as you co-wrote & co-produced several songs on the album. Do you have a favorite moment that really stands out to you from working on this project?\nLaura Bell Bundy: I have many, working on the chorus of “Two Step” on an airplane back from London with Andy Davis trying not to wake people up who were trying to sleep. Writing “China & Wine” with Natalie Hemby.. It was almost a spiritual experience, and so was the recording of that song. Nathan Chapman, all the musicians and I both felt the intensity of that moment… and we were all so connected to the emotion of the song together in the studio. Co-directing and producing the music videos with Becky Fluke was also a highlight. I love to add a visual aide to the music.\nCMR: Women in country are starting to take a strong role once again in the genre. Who are some of your favorite country female artists that you gravitate towards and that have some influence on you & your music?\nCMR: For folks still getting to know you, what is one thing that you would like to convey to country music fans about you & your new music?\nLaura Bell Bundy: I just want to connect with them. Hopefully they feel like my music reaches them in an honest organic way. Whether it makes them want to dance or sheds new light on an uncomfortable subject. I hope to provide a service to them that improves their lives just a little.\nCMR: We all have wish lists & bucket lists, so as an artist, what is something that is pretty high up there on your country music bucket list?\nCMR: Music is consistently evolving & growing. In your opinion, what do you think separates country music apart from any other genre?\nLaura Bell Bundy: Country music is about telling stories. Unlike other genres, the lyrics are of upmost importance. Country music and the artists are approachable unlike a mysterious rock or pop artist. They stay real, grounded, rooted and personable. Country Music is real people telling stories about real people.\nPick up Laura Bell Bundy’s new album ANOTHER PIECE OF ME on iTunes HERE.."

    # When
    res = classify(text, V1_EMBEDDING, 2)

    # Then
    assert len(res) == 2
    assert res == ["Music and Mind", "Discography"]
