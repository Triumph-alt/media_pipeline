#include <cstdio>

extern "C" {
#include <libavcodec/avcodec.h>
#include <libavformat/avformat.h>
#include <libswscale/swscale.h>
}

int main() {
    printf("Media Pipeline — Hello World\n");
    printf("  libavcodec:   %s\n", avcodec_configuration());
    printf("  libavformat:  %s\n", avformat_configuration());
    printf("  libswscale:   %s\n", swscale_configuration());
    return 0;
}
