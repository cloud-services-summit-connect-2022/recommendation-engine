package org.globex.retail.streams.model;

public class ProductLikes {

    private String productId;

    private int likes;

    private String category;

    public String getProductId() {
        return productId;
    }

    public int getLikes() {
        return likes;
    }

    public String getCategory() {
        return category;
    }

    public static ProductLikes sum(ProductLikes p1, ProductLikes p2) {
        Builder builder = newBuilder(p1);
        builder.likes(p1.getLikes() + p2.getLikes());
        return builder.build();
    }

    public static Builder newBuilder(ProductLikes productLikes) {
        return new Builder(productLikes.getProductId())
                .likes(productLikes.getLikes());
    }

    public static class Builder {

        private ProductLikes productLikes;

        public Builder(String productId) {
            productLikes = new ProductLikes();
            productLikes.productId = productId;
            productLikes.category = "product";
            productLikes.likes = 1;
        }

        public Builder likes(int likes) {
            productLikes.likes = likes;
            return this;
        }

        public ProductLikes build() {
            return productLikes;
        }

    }
}
