package software.amazon.athena.datacatalog;


import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.athena.model.Tag;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class TranslatorTest {

    @Test
    public void convertToAthenaSdkTags_tests() {

        ResourceModel resourceModel = BaseHandlerTest.buildTestResourceModel();
        Map<String, String> stackTags = new HashMap<>();

        //Case 0: Empty test.
        List<Tag> tags = Translator.convertToAthenaSdkTags(Collections.emptyList(), stackTags);
        assertThat(tags).isNull();

        //Case 1: No stack tags
        tags = Translator.convertToAthenaSdkTags(resourceModel.getTags(), stackTags);
        assertThat(tags.size()).isEqualTo(1);

        //Case 2: Valid stack tags
        stackTags.put("StackKey", "StackValue");
        tags = Translator.convertToAthenaSdkTags(resourceModel.getTags(), stackTags);
        assertThat(tags.size()).isEqualTo(2);

        //Case 3: Stack tag overridden by resource tag.
        stackTags.put("testKey1", "StackTagValue");
        tags = Translator.convertToAthenaSdkTags(resourceModel.getTags(), stackTags);
        assertThat(tags.size()).isEqualTo(2);
        Optional<String> value = tags.stream().filter(tag -> "testKey1".equals(tag.key())).map(Tag::value).findFirst();
        assertThat(value.isPresent()).isTrue();
        assertThat("someValue1".equals(value.get())).isTrue();
    }
}
