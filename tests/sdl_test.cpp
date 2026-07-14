#include <SDL3/SDL.h>
#include <cstdio>

int main() {
    SDL_Init(SDL_INIT_VIDEO);

    SDL_Window* window = SDL_CreateWindow("SDL3 Test", 320, 240, 0);
    if (!window) {
        fprintf(stderr, "CreateWindow failed: %s\n", SDL_GetError());
        return 1;
    }

    SDL_Renderer* renderer = SDL_CreateRenderer(window, "software");
    if (!renderer) {
        fprintf(stderr, "CreateRenderer(software) failed: %s\n", SDL_GetError());
        renderer = SDL_CreateRenderer(window, nullptr);
    }
    if (!renderer) {
        fprintf(stderr, "CreateRenderer(default) failed: %s\n", SDL_GetError());
        return 1;
    }
    fprintf(stderr, "renderer: %s\n", SDL_GetRendererName(renderer));

    // 红色
    SDL_SetRenderDrawColor(renderer, 255, 0, 0, 255);
    bool ok1 = SDL_RenderClear(renderer);
    fprintf(stderr, "RenderClear: %s\n", ok1 ? "ok" : SDL_GetError());
    SDL_RenderPresent(renderer);

    // 5 秒后退出
    SDL_Delay(5000);

    SDL_DestroyRenderer(renderer);
    SDL_DestroyWindow(window);
    SDL_Quit();
    return 0;
}
