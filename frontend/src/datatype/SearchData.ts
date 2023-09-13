export interface SearchResult {
  url: string;
	title: string;
	type: string;
	displayText: string;
  totalScore: number;
  features: {[key: string]: number}
}

export interface VideoItemId {
  kind: string;
  videoId: string;
}

export interface VideoItemSnippet {
  title: string;
  channelTitle: string;
  thumbnails: VideoThumbnails;
}

export interface VideoThumbnail {
  height: number;
  width: number;
  url: string;
}

export interface VideoThumbnails {
  default: VideoThumbnail;
  high: VideoThumbnail;
  medium: VideoThumbnail;
}

export interface VideoItem {
  id: VideoItemId;
  snippet: VideoItemSnippet;
}