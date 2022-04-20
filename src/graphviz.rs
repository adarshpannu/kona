
pub fn htmlify(s: String) -> String {
    s.replace("&", "&amp;").replace(">", "&gt;").replace("<", "&lt;")
}

