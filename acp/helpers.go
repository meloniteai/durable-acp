package acp

func TextBlock(text string) ContentBlock {
	return ContentBlock{Text: &ContentBlockText{
		Text: text,
		Type: "text",
	}}
}

func ImageBlock(data, mimeType string) ContentBlock {
	return ContentBlock{Image: &ContentBlockImage{
		Data:     data,
		MimeType: mimeType,
		Type:     "image",
	}}
}

func AudioBlock(data, mimeType string) ContentBlock {
	return ContentBlock{Audio: &ContentBlockAudio{
		Data:     data,
		MimeType: mimeType,
		Type:     "audio",
	}}
}

func ResourceLinkBlock(name, uri string) ContentBlock {
	return ContentBlock{ResourceLink: &ContentBlockResourceLink{
		Name: name,
		Type: "resource_link",
		Uri:  uri,
	}}
}

func ResourceBlock(resource EmbeddedResourceResource) ContentBlock {
	return ContentBlock{Resource: &ContentBlockResource{
		Resource: resource,
		Type:     "resource",
	}}
}

func Ptr[T any](value T) *T {
	return new(value)
}
