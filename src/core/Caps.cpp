#include "pipeline/core/Caps.h"

#include <algorithm>
#include <cstring>

namespace pipeline {

bool ChannelLayout::fromAV(const AVChannelLayout& source, ChannelLayout* out) {
    // 检查源 AVChannelLayout 是否有效
    if (!out || !av_channel_layout_check(&source)) {
        return false;
    }

    // 复制 order 和 channel 数量
    ChannelLayout converted;
    converted.order = source.order;
    converted.channels = source.nb_channels;

    // 根据 order 保存具体布局信息
    switch (source.order) {
        case AV_CHANNEL_ORDER_NATIVE:
        case AV_CHANNEL_ORDER_AMBISONIC:
            converted.mask = source.u.mask;
            break;

        case AV_CHANNEL_ORDER_CUSTOM:
            if (!source.u.map) {
                return false;
            }
            converted.custom_order.reserve(static_cast<size_t>(source.nb_channels));
            for (int index = 0; index < source.nb_channels; ++index) {
                converted.custom_order.push_back(source.u.map[index].id);
            }
            break;

        case AV_CHANNEL_ORDER_UNSPEC:
            break;

        default:
            return false;
    }

    *out = std::move(converted);
    return true;
}

bool ChannelLayout::toAV(AVChannelLayout* out) const {
    if (!out || !isValid()) {
        return false;
    }

    *out = {};
    switch (order) {
        case AV_CHANNEL_ORDER_NATIVE:
        case AV_CHANNEL_ORDER_AMBISONIC:
            if (av_channel_layout_from_mask(out, mask) < 0) {
                return false;
            }
            // av_channel_layout_from_mask creates NATIVE; restore the valid ambisonic order only
            // when the stored value really represents one. Current AudioPlay rejects ambisonic input.
            if (order == AV_CHANNEL_ORDER_AMBISONIC) {
                av_channel_layout_uninit(out);
                return false;
            }
            return true;

        case AV_CHANNEL_ORDER_CUSTOM:
            if (av_channel_layout_custom_init(out, channels) < 0) {
                return false;
            }
            for (int index = 0; index < channels; ++index) {
                out->u.map[index].id = custom_order[static_cast<size_t>(index)];
            }
            return true;

        case AV_CHANNEL_ORDER_UNSPEC:
            out->order = AV_CHANNEL_ORDER_UNSPEC;
            out->nb_channels = channels;
            return true;

        default:
            return false;
    }
}

bool ChannelLayout::isValid() const {
    if (channels <= 0) {
        return false;
    }

    switch (order) {
        case AV_CHANNEL_ORDER_NATIVE:
            return mask != 0 && __builtin_popcountll(mask) == channels;

        case AV_CHANNEL_ORDER_CUSTOM:
            return custom_order.size() == static_cast<size_t>(channels) &&
                   std::all_of(custom_order.begin(), custom_order.end(),
                               [](AVChannel channel) { return channel != AV_CHAN_NONE; });

        case AV_CHANNEL_ORDER_AMBISONIC:
            return mask != 0;

        case AV_CHANNEL_ORDER_UNSPEC:
            return true;

        default:
            return false;
    }
}

bool ChannelLayout::operator==(const ChannelLayout& other) const {
    return order == other.order &&
           channels == other.channels &&
           mask == other.mask &&
           custom_order == other.custom_order;
}

ChannelLayout ChannelLayout::stereo() {
    ChannelLayout layout;
    layout.order = AV_CHANNEL_ORDER_NATIVE;
    layout.channels = 2;
    layout.mask = AV_CH_LAYOUT_STEREO;
    return layout;
}

bool CapsEvent::hasSameFormat(const CapsEvent& other) const {
    if (media_type != other.media_type) {
        return false;
    }

    switch (media_type) {
        case MediaType::VIDEO_RAW:
            return width == other.width && height == other.height && pix_fmt == other.pix_fmt;

        case MediaType::AUDIO_RAW:
            return sample_rate == other.sample_rate && sample_fmt == other.sample_fmt &&
                   channel_layout == other.channel_layout;

        case MediaType::VIDEO_ENCODED:
            return codec_id == other.codec_id && width == other.width &&
                   height == other.height && extradata == other.extradata;

        case MediaType::AUDIO_ENCODED:
            return codec_id == other.codec_id && sample_rate == other.sample_rate &&
                   channel_layout == other.channel_layout && extradata == other.extradata;

        case MediaType::CONTAINER:
            return true;
    }
    return false;
}

} // namespace pipeline
