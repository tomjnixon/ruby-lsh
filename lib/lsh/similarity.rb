# ruby-lsh
#
# Copyright (c) 2012 British Broadcasting Corporation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Contains classes for comparing similarity of two vectors.
# Implementations should subclass SimilarityBase, implementing either just
# 'similarity', or both 'similarity' and 'similarity_list'.

# 'similarity' should accept two vectors, and return a float, which is larger
# for more similar vectors.
# 'similarity_list' maps 'similarity' over a list of vectors; this is here to
# allow an optimisation of CosineSimilarity.

module LSH

  class SimilarityBase

    def similarity_list(as, b)
      as.map { |a| similarity(a, b) }
    end

    def similarity(a, b)
      raise "Not implemented."
    end

  end

  class CosineSimilarity < SimilarityBase

    def similarity_list(as, b)
      # Transposing takes roughly the same amount of time as the dot product;
      # only do it once for the whole list.
      b_t = b.transpose
      as.map { |a| MathUtil.dot(a, b_t) }
    end

    def similarity(a, b)
      MathUtil.dot(a, b.transpose)
    end

  end

end
