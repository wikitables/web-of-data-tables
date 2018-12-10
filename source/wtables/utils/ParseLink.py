from urllib.parse import quote

def wikiLink(link):
    baseLink="https://en.wikipedia.org/wiki"
    link_ = link
    link_ = link_.replace("\"", "")
    if ("index.php" in link_):
        return ""
    if ("File:" in link_):
        return ""
    if "#" in link_:
        link_ = link_[0:link_.index("#")]
    if ("%23" in link_):
        link_ = link_[0:link_.index("%23")]
    if (link_.startswith("http")) :
        if ("wikipedia" in link_):
            return link_
    else:
        if (link_.startswith(".")):
            link_ = link_.replace(".", "")
        if (link_.startswith("/")):
            link_ = link_.replace("//", "/")
    if link_ == "":
        return ""
    if "%" in link_:
        print("Converted link: ", link_)
        return baseLink+link_
    else:
        return quote(baseLink+link_, safe='/:,()')
